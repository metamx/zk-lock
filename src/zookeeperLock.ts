import { EventEmitter } from 'events';
import * as Promise from 'bluebird';
import * as zk from 'node-zookeeper-client';
import * as util from 'util';
import { Locator } from 'locators';

const debuglog = util.debuglog('zk-lock');

/**
 * Error thrown by locking action when blocking wait for lock reaches a timeout period
 */
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

/**
 * Error thrown by locking action when config.failImmediate == true when a lock is already locked
 */
export class ZookeeperLockAlreadyLockedError extends Error {
    lockPath : string;

    constructor(message : string, path : string) {
        super(message);
        this.message = message;
        this.lockPath = path;
    }
}

export class Configuration {
    serverLocator? : Locator;
    pathPrefix? : string;
    sessionTimeout? : number;
    spinDelay? : number;
    retries? : number;
    failImmediate? : boolean;
    maxConcurrentHolders? : number;
}


export class ZookeeperLock extends EventEmitter {
    path : string;
    key : string;

    private config : Configuration = null;
    public client : zk.Client = null;
    private timedOut : boolean = false;
    private locked : boolean = false;
    private retryCount : number = 0;
    private destroyed : boolean = false;

    private timeout : number;

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
        this.config = config ? config : {};
        if (this.config.sessionTimeout == null) {
            this.config.sessionTimeout = 15000;
        }
        if (this.config.spinDelay == null) {
            this.config.spinDelay = 0;
        }
        if (this.config.retries == null) {
            this.config.retries = 0;
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
    private createClient() : Promise<any>|any {
        debuglog('creating client');
        if (this.client != null) {
            debuglog('client already created');
            return Promise.resolve(true);
        } else {
            return this.config.serverLocator().then((location) => {
                let server = location.host;
                if (location.port) {
                    server += ":" + location.port;
                }
                debuglog('server location resolved');
                debuglog(server);
                const client = zk.createClient(server, {
                    sessionTimeout: this.config.sessionTimeout,
                    spinDelay: this.config.spinDelay,
                    retries: this.config.retries
                });

                this.client = client;

                this.client.once('expired', () => {
                    if (this.key && this.path) {
                        debuglog(`${this.path}/${this.key}: expired`);
                    } else {
                        debuglog('expired');
                    }
                    this.emit(ZookeeperLock.Signals.LOST);
                    if (this.client) {
                        debuglog('removing listeners');
                        this.client.removeAllListeners();
                        this.removeAllListeners();
                        this.client.close();
                        this.client = null;
                    }
                });

                this.client.once('disconnected', this.reconnect);

                return true;
            });
        }
    };


    /**
     * connect underlying zookeeper client, with optional delay
     * @param [delay=0]
     * @returns {Promise<any>}
     */
    public connect = (delay : number = 0) : Promise<any> => {
        if (this.destroyed) {
            return Promise.reject('cannot connect, lock destroyed');
        }
        debuglog('connecting...');
        return Promise.delay(delay).then(() => {
            if (this.client && (<any>this.client.getState()).name === 'SYNC_CONNECTED') {
                debuglog('already connnected');
                return true;
            }

            if (this.client === null) {
                debuglog('client null, creating...');
                return this.createClient().then(() => {
                    return this.connectHelper();
                });
            }

            return this.connectHelper();
        });
    };

    private connectHelper = () : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            this.client.once('connected', () => {
                debuglog('connected');
                if (this.destroyed) {
                    this.disconnect().finally(() => {
                        reject('lock destroyed while connecting');
                    });
                } else {
                    resolve(true);
                }
            });

            this.client.connect();
        });
    };


    /**
     * disconnect zookeeper client, and remove all event listeners from it
     * @returns {Promise<any>}
     */
    public disconnect = () : Promise<any> => {
        if (this.key && this.path) {
            debuglog(`${this.path}/${this.key}: disconnecting...`);
        } else {
            debuglog('disconnecting...');
        }
        if (this.client == null) {
            return Promise.resolve(null);
        } else {
            this.client.removeListener('disconnected', this.reconnect);
            const timeout = this.timeout ? this.timeout : 5000;
            return this.disconnectHelper()
                .timeout(timeout, `failed to disconnect within ${timeout / 1000} seconds, returning anyway`)
                .catch(Promise.TimeoutError, (e) => {
                    debuglog(e && e.message ? e.message : e);
                    if (this.client) {
                        this.client.removeAllListeners();
                    }
                    return true;
                });
        }
    };

    private disconnectHelper = () : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            this.client.once('disconnected', () => {
                if (this.client) {
                    this.client.removeAllListeners();
                }
                this.client = null;
                if (this.key && this.path) {
                    debuglog(`${this.path}/${this.key}: disconnected`);
                } else {
                    debuglog('disconnected');
                }
                resolve(true);

            });
            this.client.close();
        });
    };


    /**
     * destroy the lock, disconnect and remove all listeners from the 'signal' event emitter
     * @returns {Promise<any>}
     */
    public destroy = () : Promise<boolean> => {
        this.destroyed = true;
        return this.disconnect().then(() => {
            if (this.key && this.path) {
                debuglog(`${this.path}/${this.key}: destroyed`);
            } else {
                debuglog(`destroyed`);
            }
            this.removeAllListeners();
            // wait for session timeout for ephemeral lock to go away
            // return Promise.delay(this.config.sessionTimeout).thenReturn(true);
            return true;
        });
    };


    /**
     * internal method to reconnect, wired up to disconnect event of zk client
     * @returns {Promise<any>}
     */
    private reconnect = () : Promise<any> => {
        this.retryCount++;
        if (this.destroyed || this.config.failImmediate || (this.retryCount <= this.config.retries) || !this.locked) {
            return Promise.resolve(false);
        }
        debuglog(`reconnecting ${this.retryCount}...`);
        return this.connect(this.config.spinDelay);
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
            const cleanup = () => {
                let destroyFunc : () => Promise<any>;

                if (destroy) {
                    destroyFunc = this.destroy;
                } else {
                    destroyFunc = this.disconnect;
                }

                destroyFunc().then(() => {
                    if (this.path && this.key) {
                        debuglog(`${this.path}/${this.key}: unlocked, cleanup complete`);
                    } else {
                        debuglog('cleanup complete');
                    }
                    resolve(true);
                }).catch(() => {
                    debuglog('cleanup failed');
                    reject(false);
                });
            };

            if (this.client) {
                if (this.path && this.key) {
                    debuglog(`${this.path}/${this.key}: unlocking..,`);
                    this.client.remove(
                        `${this.path}/${this.key}`,
                        (err) => {
                            if (err && err.message && err.message.indexOf('NO_NODE') < 0) {
                                debuglog(`${this.path}/${this.key}: failed to remove due to: ${err.message}.`);
                            }
                            cleanup();
                        }
                    );
                } else {
                    debuglog(`lock not set, skipping unlock, but cleaning up connection`);
                    cleanup();
                }
            } else {
                debuglog(`client not connected, skipping unlock and cleanup`);
                resolve(true);
            }
        });
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
        const someRandomExtraLogText = this.config.maxConcurrentHolders > 1 ?
            ` with ${this.config.maxConcurrentHolders} concurrent lock holders` :
            '';

        if (this.locked) {
            debuglog('already locked');
            return Promise.resolve(this);
        }

        this.timeout = timeout;

        debuglog(`try locking ${key} at ${path}${someRandomExtraLogText}`);

        if (timeout && !this.config.failImmediate) {
            return this.lockHelper(path, nodePath, timeout)
                .timeout(timeout, 'timeout')
                .catch(Promise.TimeoutError, (te) => {
                    this.timedOut = true;
                    this.disconnect();
                    throw new ZookeeperLockTimeoutError('timeout', path, timeout);
                });
        } else {
            return this.lockHelper(path, nodePath);
        }

    };

    private lockHelper = (path : string, nodePath : string, timeout? : number) : Promise<any> => {
        return this.connect()
            .then(() => {
                this.timedOut = false;
                debuglog(`making lock at ${nodePath}`);
                return this.makeLockDir(nodePath);
            })
            .then(() => {
                return this.initLock(nodePath);
            })
            .then(() => {
                debuglog(`${this.path}/${this.key}: waiting for lock`);
                return this.waitForLock(nodePath);
            })
            .then(() => {
                this.locked = true;
                debuglog(`${this.path}/${this.key}: lock acquired`);
                return this;
            }).catch((err) => {
                this.locked = false;
                debuglog(`${this.path}/${this.key}: error grabbing lock: ${err.message}`);
                throw err;
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
                    if (this.checkBail()) {
                        return reject(new Error('aborting lock process'));
                    }

                    if (err) {
                        return reject(new Error(`Failed to create directory: ${path} due to: ${err}.`));
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
                    if (this.checkBail()) {
                        return reject(new Error('aborting lock process'));
                    }

                    if (err) {
                        return reject(new Error(`Failed to create node: ${lockPath} due to: ${err}.`));
                    }
                    debuglog(`init lock: ${path}, ${lockPath.replace(path + '/', '')}`);

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
    private waitForLock = (path) : Promise<any> => {
        return new Promise<any>((resolve, reject) => {
            this.waitForLockHelper(resolve, reject, path);
        });
    };


    private checkBail = () => {
        return this.timedOut || this.destroyed || (this.client && (<any>this.client.getState()).name !== 'SYNC_CONNECTED');
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
        debuglog(`${path} wait loop.`);
        if (this.locked || this.checkBail()) {
            return reject(new Error('aborting lock process'));
        }

        this.client.getChildren(
            path,
            (event) => {
                if (this.locked || this.checkBail()) {
                    return reject(new Error('aborting lock process'));
                }
                debuglog(`${path}/${this.key}: children changed.`);
                this.waitForLockHelper(resolve, reject, path);
            },
            (err, locks, state) => {
                if (this.locked || this.checkBail()) {
                    return reject(new Error('aborting lock process'));
                }
                try {
                    if (err || !locks || locks.length === 0) {
                        const errMsg = err && err.message ? err.message : 'no children';
                        debuglog(`${path}/${this.key}: failed to get children: ${errMsg}`);
                        return reject(new Error(`Failed to get children node: ${errMsg}.`));
                    }

                    const sequence = this.filterLocks(locks)
                        .map((l) => {
                            return ZookeeperLock.getSequenceNumber(l);
                        })
                        .filter((l) => {
                            return l >= 0;
                        });

                    debuglog(`${path}/${this.key}: lock sequence: ${JSON.stringify(sequence)}`);

                    const mySeq = ZookeeperLock.getSequenceNumber(this.key);

                    // todo: fix me for max concurrent holders...
                    // make sure we are first...
                    const min = sequence.reduce((acc, elem) => {
                        return Math.min(acc, elem);
                    }, mySeq);

                    debuglog(`${path}/${this.key}: checking ${mySeq} less than ${min} + ${this.config.maxConcurrentHolders}`);
                    if (mySeq < (min + this.config.maxConcurrentHolders)) {
                        debuglog(`${path}/${this.key}: ${mySeq} can grab the lock on ${path}`);
                        return resolve(true);
                    } else if (this.config.failImmediate) {
                        this.locked = false;
                        this.destroyed = true;
                        debuglog(`${path}/${this.key}: failing immediately`);
                        return reject(new ZookeeperLockAlreadyLockedError('already locked', path));
                    }
                    this.locked = false;
                    debuglog(`${path}/${this.key}: lock not available for ${mySeq} on ${path}, waiting...`);
                } catch (ex) {
                    debuglog(`${path}/${this.key}: error - ${ex.message}`);
                    reject(ex);
                }
            }
        );
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
        return this.connect()
            .then(() => {
                return this.checkedLockedHelper(key);
            })
            .catch((err) => {
                if (err && err.message && err.message.indexOf('NO_NODE') > -1 ) {
                    return false;
                } else {
                    debuglog(`error checking locked: ${key}: ${err && err.message ? err.message : 'unknown'}`);
                    throw err;
                }
            });
    };

    private checkedLockedHelper = (key : string) => {
        return new Promise<boolean>((resolve, reject) => {
            const path = `/locks/${this.config.pathPrefix ? this.config.pathPrefix + '/' : '' }`;
            const nodePath = `${path}${key}`;
            this.client.getChildren(
                nodePath,
                null,
                (err, locks, stat) => {
                    if (err) {
                        reject(err);
                    } else if (locks) {

                        const filtered = this.filterLocks(locks);

                        debuglog(`check ${nodePath}: ${JSON.stringify(filtered)}`);

                        if (filtered && (filtered.length - (this.config.maxConcurrentHolders - 1) > 0)) {
                            debuglog(`check ${nodePath}: no locks held`);
                            resolve(true);
                        } else {
                            debuglog(`check ${nodePath}: ${filtered.length} locks held`);
                            resolve(false);
                        }
                    } else {
                        resolve(false);
                    }
                }
            );
        });
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
        const zkLock = new ZookeeperLock(ZookeeperLock.config);

        return zkLock.lock(key, timeout).catch((err) => {
            zkLock.destroy();
            throw err;
        });
    };

    /**
     * check if a lock exists for a path using the static config
     * @param key
     * @returns {Promise<boolean>}
     */
    public static checkLock = (key : string) : Promise<boolean> => {
        const zkLock = new ZookeeperLock(ZookeeperLock.config);

        return zkLock.checkLocked(key)
            .then((result) => {
                return result ? true : false;
            })
            .finally(() => {
                zkLock.destroy();
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
