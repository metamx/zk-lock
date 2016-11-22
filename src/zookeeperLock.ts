import { EventEmitter } from 'events';
import * as Promise from 'bluebird';
import * as zk from 'node-zookeeper-client';
import * as util from 'util';
import { Locator } from 'locators';

const debuglog = util.debuglog('zk-lock');

export class ZookeeperLockTimeoutError extends Error {
    lockPath : string;
    timeout? : number;

    constructor(message : string, path : string, timeout? : number) {
        super(message);
        this.message = message;
        this.lockPath = path;
        this.timeout = timeout;
    }
}

export class Configuration {
    serverLocator : Locator;
    pathPrefix : string;
    sessionTimeout? : number;
    spinDelay? : number;
    retries? : number;
    maxConcurrentHolders? : number;
}

export class ZookeeperLock extends EventEmitter {
    path : string;
    key : string;

    private config : Configuration = null;
    public client : zk.Client = null;
    private connected : boolean = false;
    private static config : Configuration = null;

    public static Signals = {
        LOST: 'lost',
        TIMEOUT: 'timeout'
    };

    /**
     * create a new zk lock
     * @param config
     */
    constructor(config : Configuration) {
        super();
        this.config = config;
        if (this.config.sessionTimeout == null) {
            this.config.sessionTimeout = 15000;
        }
        if (this.config.spinDelay == null) {
            this.config.spinDelay = 1000;
        }
        if (this.config.retries == null) {
            this.config.retries = 3;
        }
        if (this.config.maxConcurrentHolders == null) {
            this.config.maxConcurrentHolders = 1;
        }

        debuglog(JSON.stringify(this.config));
    }

    /**
     * create a zookeeper client which powers the lock, done when creating a new lock
     * or zk connection expires
     * @returns {Promise<any>}
     */
    private createClient() : Promise<any> {
        debuglog('creating client');
        return new Promise<any>((resolve, reject) => {
            if (this.client != null) {
                debuglog('client already created');
                resolve(true);
            } else {
                this.config.serverLocator().then((location) => {
                    let server = location.host;
                    if (location.port) {
                        server += ":" + location.port;
                    }
                    debuglog(server);
                    const client = zk.createClient(server, {
                        sessionTimeout: this.config.sessionTimeout,
                        spinDelay: this.config.spinDelay,
                        retries: this.config.retries
                    });

                    this.client = client;

                    this.client.once('expired', () => {
                        debuglog('expired');
                        this.emit(ZookeeperLock.Signals.LOST);
                        if (this.client) {
                            this.client.removeAllListeners();
                            this.removeAllListeners();
                            this.client = null;
                        }
                        this.createClient();
                    });

                    this.client.once('disconnected', this.reconnect);

                    resolve(true);
                });
            }
        });
    };


    /**
     * connect underlying zookeeper client, with optional delay
     * @param [delay=0]
     * @returns {Promise<any>}
     */
    public connect = (delay : number = 0) : Promise<any> => {
        debuglog('connecting...');
        return new Promise<any>((resolve, reject) => {
            setTimeout(() => {
                try {
                    if (this.connected) {
                        debuglog('already connnected');
                        resolve(true);
                        return;
                    }

                    if (this.client === null) {
                        this.createClient().then(() => {
                            return this.connect();
                        }).then(() => {
                            resolve(true);
                        }).catch((err) => {
                            reject(err);
                        });

                        return;
                    }

                    this.client.once('connected', () => {
                        this.connected = true;
                        resolve(true);
                    });

                    this.client.connect();
                } catch (ex) {
                    reject(ex);
                }
            }, delay);
        });
    };

    /**
     * disconnect zookeeper client, and remove all event listeners from it
     * @returns {Promise<any>}
     */
    public disconnect = () : Promise<any> => {
        debuglog('disconnecting...');
        return new Promise<any>((resolve, reject) => {
            this.client.removeListener('disconnected', this.reconnect);
            this.client.once('disconnected', () => {
                if (this.client) {
                    this.client.removeAllListeners();
                }
                this.client = null;
                this.connected = false;
                debuglog('disconnected');
                resolve(true);
            });
            this.client.close();

            setTimeout(() => {
                resolve(true);
            }, 5000);
        });
    };

    /**
     * internal method to reconnect, wired up to disconnect event of zk client
     * @returns {Promise<any>}
     */
    private reconnect = () : Promise<any> => {
        debuglog('reconnecting...');
        this.connected = false;
        return this.connect(this.config.spinDelay);
    };

    /**
     * wait for a lock to become free for a given key and acquire it, with an optional
     * timeout upon which the lock will fail. if not currently connected to zookeeper,
     * this will connect, and on timeout, the lock will disconnect from zookeeper
     * @param key
     * @param [timeout]
     * @returns {Promise<any>}
     */
    public lock = (key : string, timeout : number = 0) : Promise<any> => {
        const path = `/locks/${this.config.pathPrefix ? this.config.pathPrefix + '/' : '' }`;
        const nodePath = `${path}${key}`;
        debuglog(`try locking ${key} at ${path}${this.config.maxConcurrentHolders > 1 ? ` with ${this.config.maxConcurrentHolders} concurrent lock holders` : ''}`);

        return new Promise<any>((resolve, reject) => {
            let timedOut = false;
            if (timeout) {
                debuglog('starting timeout');
                setTimeout(() => {
                    debuglog('timed out, cancelling lock.');
                    timedOut = true;
                    this.emit(ZookeeperLock.Signals.TIMEOUT);
                    reject(new ZookeeperLockTimeoutError('timeout', path, timeout));
                 }, timeout);
            }
            this.connect()
            .then(() => {
                debuglog(`making lock at ${nodePath}`);
                return this.makeLockDir(nodePath);
            })
            .then(() => {
                return this.initLock(nodePath);
            })
            .then(() => {
                debuglog(`waiting for lock at ${nodePath}`);
                return this.waitForLock(nodePath, timeout);
            })
            .then((lock : ZookeeperLock) => {
                debuglog('lock acquired');
                resolve(true);
            }).catch((err) => {
                debuglog('error grabbing lock:' + err);
                if (timedOut) {
                    this.disconnect().then(() => {
                        reject(err);
                    }).catch((err2) => {
                        reject(err2);
                    });
                } else {
                    reject(err);
                }
            });
        });
    };

    /**
     * unlock a lock, removing the key from zookeeper, and disconnecting
     * the zk client and all event listeners. By default this also destroys
     * the lock and removes event listeners on the locks 'signals' event
     * @param [destroy=true] - remove listeners from lock in addition
     * to disconnecting zk client on completion, defaults to true
     * @returns {Promise<any>}
     */
    public unlock = (destroy : boolean = true) : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            if (this.client) {
                this.client.remove(
                    `${this.path}/${this.key}`,
                    (err) => {
                        if (err && err.message.indexOf('NO_NODE') < 0) {
                            reject(new Error(`Failed to remove children node: ${this.path}/${this.key} due to: ${err}.`));
                            return;
                        }

                        let destroyFunc : () => Promise<any>;

                        if (destroy) {
                            destroyFunc = this.destroy;
                        } else {
                            destroyFunc = this.disconnect;
                        }

                        destroyFunc().then(() => {
                            resolve(true);
                        }).catch(() => {
                            reject(false);
                        });
                    }
                );
            } else {
                resolve(true);
            }
        });
    };

    /**
     * destroy the lock, disconnect and remove all listeners from the 'signal' event emitter
     * @returns {Promise<any>}
     */
    public destroy = () : Promise<boolean> => {
        return new Promise<any>((resolve, reject) => {
            this.disconnect().then(() => {
                this.removeAllListeners();
                // wait for session timeout for ephemeral lock to go away
                setTimeout(() => {
                    resolve(true);
                }, this.config.sessionTimeout);
            }).catch(() => {
                reject(false);
            });
        });
    };

    /**
     * method to filter zk node children to contain only those that are prefixed with 'lock-',
     * which are assumed to be created by this library
     * @param children
     * @returns {string[]|T[]}
     */
    private filterLocks = (children : Array<string>) : Array<string> => {
        const filtered = children.filter((l) => {
            return l !== null && l.indexOf('lock-') === 0;
        });

        return filtered;
    };


    /**
     * check if a lock exists, connecting to zk client if not connected
     * @param key
     * @returns {Promise<boolean>}
     */
    public checkLocked = (key : string) : Promise<boolean> => {
        return new Promise<boolean>((resolve, reject) => {
            this.connect()
                .then(() => {
                    const path = `/locks/${this.config.pathPrefix ? this.config.pathPrefix + '/' : '' }`;
                    const nodePath = `${path}${key}`;
                    this.client.getChildren(
                        nodePath,
                        null,
                        (err, locks, stat) => {
                            if (err) {
                                debuglog(err.message);
                                reject(err);
                            }
                            if (locks) {

                                const filtered = this.filterLocks(locks);

                                debuglog(JSON.stringify(filtered));

                                if (filtered && filtered.length > 0) {
                                    resolve(true);
                                } else {
                                    resolve(false);
                                }
                            } else {
                                resolve(false);
                            }
                        });
                })
                .catch((err) => {
                    if (err.indexOf('NO_NODE') > -1 ) {
                        resolve(false);
                    } else {
                        debuglog(err.message);
                        reject(err);
                    }
                });
        });
    };

    /**
     * make the zk node that will hold the locks if it doens't already exist
     * @param path
     * @returns {Promise<any>}
     */
    private makeLockDir = (path) : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            this.client.mkdirp(
                path,
                (err) => {
                    if (err) {
                        reject(new Error(`Failed to create directory: ${path} due to: ${err}.`));
                        return;
                    }
                    resolve(true);
                }
            );
        });
    };


    /**
     * create a lock as a ephemeral sequential child node of the supplied path, prefixed with 'lock-',
     * to state intent to acquire a lock
     * @param path
     * @returns {Promise<any>}
     */
    private initLock = (path) : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            this.client.create(
                `${path}/lock-`,
                new Buffer('lock'),
                zk.CreateMode.EPHEMERAL_SEQUENTIAL,
                (err, lockPath) => {
                    if (err) {
                        reject(new Error(`Failed to create node: ${lockPath} due to: ${err}.`));
                        return;
                    }
                    debuglog(`lock: ${path}, ${lockPath.replace(path + '/', '')}`);

                    this.path = path;
                    this.key = lockPath.replace(path + '/', '');

                    resolve(true);
                }
            );
        });
    };

    /**
     * loop until lock is available or timeout occurs
     * @param path
     * @returns {Promise<any>}
     */
    private waitForLock = (path, timeout : number) : Promise<any> => {

        return new Promise<any>((resolve, reject) => {
            this.once(ZookeeperLock.Signals.TIMEOUT, () => {
               reject(new ZookeeperLockTimeoutError('timeout', path, timeout));
            });
            this.waitForLockHelper(resolve, reject, path);
        });
    };

    /**
     * helper method that does the grunt of the work of waiting for the lock. This method does 2 things, first
     * reads the lock path to compare the locks key to the other keys that are children of the path. if this locks
     * sequence number is the lowest, the lock has been aquired. If not, this method reactively responds to
     * children changed events from the zk-client for the path we want to aqcuire the lock for, and recurses to
     * repeat this process until the sequence is the lowest
     * @param resolve
     * @param reject
     * @param path
     */
    private waitForLockHelper = (resolve, reject, path) : void => {
        debuglog('wait loop.');
        this.client.getChildren(
            path,
            (event) => {
                debuglog('children changed.');
                this.waitForLockHelper(resolve, reject, path);
            },
            (err, locks, stat) => {
                try {
                    if (err || !locks || locks.length === 0) {
                        debuglog('failed to get children:' + err);
                        if (this.connected) {
                            this.unlock().then(() => {
                                reject(new Error(`Failed to get children node: ${path} due to: ${err}.`));
                            }).catch((err2) => {
                                reject(err2);
                            });
                        } else {
                            reject(new Error(`Failed to get children node: ${path} due to: ${err}.`));
                        }
                        return;
                    }

                    const sequence = this.filterLocks(locks)
                        .map((l) => {
                            return ZookeeperLock.getSequenceNumber(l);
                        })
                        .filter((l) => {
                            return l >= 0;
                        });

                    debuglog(JSON.stringify(sequence));

                    const mySeq = ZookeeperLock.getSequenceNumber(this.key);

                    // make sure we are first...
                    const min = sequence.reduce((acc, elem) => {
                        return Math.min(acc, elem);
                    }, mySeq);

                    debuglog(`checking ${mySeq} less than ${min} + ${this.config.maxConcurrentHolders}`);
                    if (mySeq < (min + this.config.maxConcurrentHolders)) {
                        resolve(true);
                    }
                } catch (ex) {
                    debuglog(ex.message);
                    reject(ex);
                }
            }
        );
    };


    /**
     * set static config to use by static helper methods
     * @param config
     */
    public static initialize = (config : any) : void => {
        ZookeeperLock.config = config;
    };

    /**
     * create a new lock using the static stored config
     * @returns {ZookeeperLock}
     */
    public static lockFactory = () : ZookeeperLock => {
        return new ZookeeperLock(ZookeeperLock.config);
    };

    /**
     * create a new lock and lock it using the static stored config, with optional timeout
     * @param key
     * @param timeout
     * @returns {Promise<ZookeeperLock>}
     */
    public static lock = (key : string, timeout? : number) : Promise<ZookeeperLock> => {
        return new Promise<ZookeeperLock>((resolve, reject) => {
            const zkLock = new ZookeeperLock(ZookeeperLock.config);

            zkLock.lock(key, timeout)
                .then(() => {
                    resolve(zkLock);
                }).catch((err) => {
                    reject(err);
                    zkLock.destroy();
                });
        });
    };

    /**
     * check if a lock exists for a path using the static config
     * @param key
     * @returns {Promise<boolean>}
     */
    public static checkLock = (key : string) : Promise<boolean> => {


        return new Promise<boolean>((resolve, reject) => {
            const zkLock = new ZookeeperLock(ZookeeperLock.config);

            zkLock.checkLocked(key)
                .then((result) => {
                    if (result) {
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                })
                .catch((err) => {
                    reject(err);
                })
                .finally(() => {
                    zkLock.destroy();
                });
        });
    };


    /**
     * get the numeric part of the lock key
     * @param path
     * @returns {number}
     */
    private static getSequenceNumber = (path : string) : number => {
        return parseInt(path.replace('lock-', ''), 10);
    };
}
