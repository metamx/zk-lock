import { EventEmitter } from 'events';
import { Promise } from 'q';
import zk = require('node-zookeeper-client');
import util = require('util');
var debuglog = util.debuglog('zk-lock');

export class Configuration {
    serverLocator : () => Promise<any>;
    pathPrefix : string;
    sessionTimeout : number;
    spinDelay : number;
    retries : number;
}


export class ZookeeperLock {
    signal : EventEmitter;
    path : string;
    key : string;

    private config : Configuration = null;
    public client : zk.Client = null;
    private connected : boolean = false;
    private static config : Configuration = null;

    constructor(config : Configuration) {
        this.signal = new EventEmitter();
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

        debuglog(JSON.stringify(this.config));
    }

    private createClient() : Promise<any> {
        debuglog('creating client');
        return Promise<any>((resolve, reject) => {
            if (this.client != null) {
                debuglog('client already created');
                resolve(true);
            } else {
                this.config.serverLocator().then((location) => {
                    var server = location.host;
                    if (location.port) {
                        server += ":" + location.port;
                    }
                    debuglog(server);
                    var client = zk.createClient(server, {
                        sessionTimeout: this.config.sessionTimeout,
                        spinDelay: this.config.spinDelay,
                        retries: this.config.retries
                    });

                    this.client = client;

                    this.client.once('expired', () => {
                        debuglog('expired');
                        this.signal.emit('lost');
                        if (this.client) {
                            this.client.removeAllListeners();
                            this.signal.removeAllListeners();
                        }
                        this.createClient();
                    });

                    this.client.once('disconnected', this.reconnect);

                    resolve(true);
                });
            }
        });
    };


    public connect = (delay : number = 0) : Promise<any> => {
        debuglog('connecting...');
        return Promise<any>((resolve, reject) => {
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

    public disconnect = () : Promise<any> => {
        debuglog('disconnecting...');
        return Promise<any>((resolve, reject) => {
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
        });
    };

    private reconnect = () : Promise<any> => {
        debuglog('reconnecting...');
        this.connected = false;
        return this.connect(this.config.spinDelay);
    };

    public lock = (key : string, timeout? : number) : Promise<any> => {
        var path = `/locks/${this.config.pathPrefix ? this.config.pathPrefix + '/' : '' }`;
        var nodePath = `${path}${key}`;
        debuglog(`try locking ${key} at ${path}`);

        return Promise<any>((resolve, reject) => {
            var timedOut = false;
            if (timeout) {
                debuglog('starting timeout');
                setTimeout(() => {
                    debuglog('timed out, cancelling lock.');
                    timedOut = true;
                    this.signal.emit('timeout');
                 }, timeout);
            }
            this.connect()
            .then(() => {
                if (timedOut) {
                    debuglog('timed out after connect');
                    throw new Error('timeout');
                }

                debuglog(`making lock at ${nodePath}`);
                return this.makeLockDir(nodePath);
            })
            .then(() => {
                if (timedOut) {
                    debuglog('timed out after make lock dir');
                    throw new Error('timeout');
                }

                return this.initLock(nodePath);
            })
            .then(() => {
                if (timedOut) {
                    debuglog('timed out after init lock');
                    throw new Error('timeout');
                }

                debuglog(`waiting for lock at ${nodePath}`);
                return this.waitForLock(nodePath);
            })
            .then((lock : ZookeeperLock) => {
                if (timedOut) {
                    debuglog('timed out waiting for lock');
                    throw new Error('timeout');
                }

                debuglog('lock acquired');
                resolve(true);
            }).catch((err) => {
                debuglog('error grabbing lock:' + err);
                if (timedOut) {
                    this.disconnect().then(() => {
                        reject(err);
                    });
                } else {
                    reject(err);
                }
            });
        });
    };

    public unlock = () : Promise<any> => {
        return Promise<any>((resolve, reject) => {
            this.client.remove(
                `${this.path}/${this.key}`,
                (err) => {
                    if (err) {
                        reject(new Error(`Failed to remove children node: ${this.path}/${this.key} due to: ${err}.`));
                        return;
                    }

                    this.disconnect().then(() => {
                        this.signal.removeAllListeners();
                        // wait for session timeout for ephemeral lock to go away
                        setTimeout(() => {
                            resolve(true);
                        }, this.config.sessionTimeout);
                    });
                }
            );
        });
    };

    private filterLocks = (children : Array<string>) : Array<string> => {
        var filtered = children.filter((l) => {
            return l !== null && l.indexOf('lock-') === 0;
        });

        return filtered;
    };

    public checkLocked = (key : string) : Promise<boolean> => {
        return Promise<boolean>((resolve, reject) => {
            this.connect()
                .then(() => {
                    var path = `/locks/${this.config.pathPrefix ? this.config.pathPrefix + '/' : '' }`;
                    var nodePath = `${path}${key}`;
                    this.client.getChildren(
                        nodePath,
                        null,
                        (err, locks, stat) => {
                            if (err) {
                                debuglog(err.message);
                                reject(err);
                            }
                            if (locks) {

                                var filtered = this.filterLocks(locks);

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

    private makeLockDir = (path) : Promise<any> => {
        return Promise<any>((resolve, reject) => {
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

    private initLock = (path) : Promise<any> => {
        return Promise<any>((resolve, reject) => {
            this.client.create(
                `${path}/lock-`,
                new Buffer('lock'),
                zk.CreateMode.EPHEMERAL_SEQUENTIAL,
                (err, lockPath) => {
                    if (err) {
                        reject(new Error(`Failed to create node: ${lockPath} due to: ${err}.`));
                        return;
                    }
                    debuglog(`lock: ${path}, ${lockPath.replace(path + '/','')}`);

                    this.path = path;
                    this.key = lockPath.replace(path + '/', '');

                    resolve(true);
                }
            );
        });
    };

    private waitForLock = (path) : Promise<any> => {

        return Promise<any>((resolve, reject) => {
            this.signal.once('timeout', () => {
               reject(new Error('timeout'));
            });
            this.waitForLockHelper(resolve, reject, path);
        });
    };

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
                            });
                        } else {
                            reject(new Error(`Failed to get children node: ${path} due to: ${err}.`));
                        }
                        return;
                    }

                    var sequence = this.filterLocks(locks)
                        .map((l) => {
                            return ZookeeperLock.getSequenceNumber(l);
                        })
                        .filter((l) => {
                            return l >= 0;
                        });

                    debuglog(JSON.stringify(sequence));

                    var mySeq = ZookeeperLock.getSequenceNumber(this.key);

                    // make sure we are first...
                    var min = sequence.reduce((acc, elem) => {
                        return Math.min(acc, elem);
                    }, mySeq);

                    if (sequence.length === 0 || mySeq <= min) {
                        resolve(true);
                    }
                } catch (ex) {
                    debuglog(ex.message);
                    reject(ex);
                }
            }
        );
    };


    public static initialize = (config : any) : void => {
        ZookeeperLock.config = config;
    };

    public static lockFactory = () : ZookeeperLock => {
        return new ZookeeperLock(ZookeeperLock.config);
    };

    public static lock = (key : string, timeout? : number) : Promise<ZookeeperLock> => {
        return Promise<ZookeeperLock>((resolve, reject) => {
            var zkLock = new ZookeeperLock(ZookeeperLock.config);

            zkLock.lock(key, timeout)
                .then(() => {
                    resolve(zkLock);
                }).catch((err) => {
                    reject(err);
                });
        });
    };

    public static checkLock = (key : string) : Promise<boolean> => {


        return Promise<boolean>((resolve, reject) => {
            var zkLock = new ZookeeperLock(ZookeeperLock.config);

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
                    zkLock.disconnect();
                });
        });
    };


    private static getSequenceNumber = (path : string) => {
        return parseInt(path.replace('lock-', ''), 10);
    };
}
