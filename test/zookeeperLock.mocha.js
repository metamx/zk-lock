"use strict";

const expect = require('chai').expect;
const promise = require('bluebird');
const exec = require('child_process').exec;
const zookeeper = require('node-zookeeper-client');
const simple = require('locators').simple;
const lock = require('../build/zookeeperLock');
const {ZookeeperLock, ZookeeperLockTimeoutError, ZookeeperLockAlreadyLockedError} = lock;


// todo: set this to the path to your zkServer command to run tests
const zkServerCommandPath = '~/Downloads/zookeeper-3.4.6/bin/zkServer.sh';

// todo: set this to the address of your zk server if non-standard
const zkServer = 'localhost:2181';

const zkClient = zookeeper.createClient(
    zkServer,
    {
        sessionTimeout: 15000,
        spinDelay: 1000,
        retries: 0
    }
);

const simpleExec = (cmd, done) => {
    exec(cmd, (err, stdout, stderr) => {
        if (err) {
            console.log(cmd);
            console.log('  stdout: ' + stdout);
            console.log('  stderr: ' + stderr);
            console.log('  exec err: ' + err);
            done(err);
            return;
        }
        done();
    });
};


describe('sanity tests', function () {
    it('correctly strips paths from sequences', function () {
        const seq = ZookeeperLock.getSequenceNumber('lock-1');
        expect(seq).to.equal(1);
    });
});


let locator = simple()(zkServer);

const config = {
    serverLocator: locator,
    pathPrefix: 'tests',
    sessionTimeout: 2000
};


describe('Zookeeper lock', function () {
    this.timeout(120000);

    before((beforeComplete) => {

        simpleExec(zkServerCommandPath + ' start', (err) => {
            if (err) {
                beforeComplete(err);
                return;
            }
            ZookeeperLock.initialize(config);
            beforeComplete();
        });
    });

    afterEach((afterEachComplete) => {
        this.timeout(10000);
        setTimeout(() => {
            afterEachComplete();
        }, 3000);
    });

    after((afterComplete) => {
        this.timeout(20000);
        setTimeout(() => {
            simpleExec(zkServerCommandPath + ' stop', afterComplete);

            afterComplete();
        }, 3000);
    });

    it("can lock when nothing holds the lock", function (testComplete) {
        this.timeout(10000);
        ZookeeperLock.lock('test').then((lock) => {
            lock.on(ZookeeperLock.Signals.LOST, () => {
                testComplete(new Error('failed, lock should not have been lost'));
            });
            lock.unlock().then(() => {
                testComplete();
            });
        }).catch(testComplete);
        return;
    });

    it("can relock a lock that has been locked and unlocked", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test').then((lock) => {
            lock.on(ZookeeperLock.Signals.LOST, () => {
                testComplete(new Error('failed, lock should not have been lost'));
            });
            lock.unlock(false).then(() => {
                setTimeout(() => {
                    lock.lock('test').then(() => {
                        lock.unlock().then(() => {
                            testComplete();
                        });
                    });
                }, 3000);
            });
        }).catch(testComplete);
        return;
    });


    it("can get an unlocked lock and lock it", function (testComplete) {
        this.timeout(10000);
        try {
            const lock = ZookeeperLock.lockFactory();

            lock.lock('test').then(() => {
                lock.on(ZookeeperLock.Signals.LOST, () => {
                    testComplete(new Error('failed, lock should not have been lost'));
                });
                return lock.unlock();
            }).then(() => {
                testComplete();
            }).catch(testComplete);
            return;
        } catch (ex) {
            testComplete(ex);
        }
    });


    it("can not acquire a lock when something else holds it until it is released", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test').then((lock) => {
            lock.on(ZookeeperLock.Signals.LOST, () => {
                testComplete(new Error('failed, lock should not have been lost'));
            });
            let isUnlocked = false;
            ZookeeperLock.lock('test').then((lock2) => {
                lock2.on(ZookeeperLock.Signals.LOST, () => {
                    testComplete(new Error('failed, lock should not have been lost'));
                });
                expect(isUnlocked).to.be.true;
                return lock2.unlock();
            }).then(() => {
                testComplete();
            }).catch((err) => {
                testComplete(err);
            });

            setTimeout(() => {
                isUnlocked = true;
                lock.unlock().then(() => {
                })
            }, 8000);
        });
        return;
    });


    it("can check if a lock exists for a key when lock exists", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test')
            .then((lock) => {
                lock.on(ZookeeperLock.Signals.LOST, () => {
                    testComplete(new Error('failed, lock should not have been lost'));
                });
                ZookeeperLock.checkLock('test')
                    .then((result) => {
                        expect(result).to.be.true;
                        return lock.unlock();
                    }).then(() => {
                        setTimeout(() => {
                            ZookeeperLock.checkLock('test')
                                .then((result2) => {
                                    expect(result2).to.be.false;
                                    testComplete();
                                }).catch(testComplete);
                        }, 1000);
                    }).catch(testComplete);
            }).catch(testComplete);
            return;
    });

    it("can check if a lock exists for a key when lock doesn't exist", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.checkLock('noooooooo')
            .then((result) => {
                expect(result).to.be.false;
                testComplete();
            }).catch(testComplete);
        return;
    });

    it("can timeout if given a timeout to wait for a lock", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test')
            .then((lock) => {
                lock.on(ZookeeperLock.Signals.LOST, () => {
                    testComplete(new Error('failed, lock should not have been lost'));
                });
                ZookeeperLock.lock('test', 5000)
                    .then((lock2) => {
                        lock2.unlock().then(() => {
                            testComplete(new Error('did not timeout'));
                        });
                    }).catch(ZookeeperLockTimeoutError, (err) => {
                    expect(err.message).to.equal('timeout');
                    lock.unlock().then(() => {
                        testComplete();
                    });
                }).catch(testComplete);
            }).catch(testComplete);
        return;
    });


    it("does not surrender the lock on disconnect if session does not expire", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test').then((lock) => {
            lock.on(ZookeeperLock.Signals.LOST, () => {
                testComplete(new Error('failed, lock should not have been lost'));
            });

            setTimeout(() => {
                simpleExec(zkServerCommandPath + ' stop', () => {
                    setTimeout(() => {
                        simpleExec(zkServerCommandPath + ' start', () => {
                            setTimeout(() => {
                                lock.unlock().then(() => {
                                    testComplete();
                                });
                            }, 2000);
                        });
                    }, 0);
                });
            }, 0);
        });
        return;
    });

    it("releases the lock and emits the expired event on sessionTimeout", function (testComplete) {
        this.timeout(20000);
        ZookeeperLock.lock('test').then((lock) => {
            lock.on(ZookeeperLock.Signals.LOST, () => {
                testComplete();
            });

            // burn up some time to force session to timeout
            let burning = true;
            let ctime = 0;
            const time = process.hrtime();
            while (burning) {
                const nowTime = process.hrtime(time);
                if (ctime !== nowTime[0]) {
                    ctime = nowTime[0];
                }
                burning = nowTime[0] < 10;
            }
        });
        return;
    });

    it("can have concurrent lock holders if configured to allow it", function (testComplete) {
        const multiConfig = {
            serverLocator: locator,
            pathPrefix: 'tests',
            sessionTimeout: 2000,
            maxConcurrentHolders: 2
        };
        const lock1 = new ZookeeperLock(multiConfig);
        const lock2 = new ZookeeperLock(multiConfig);
        const lock3 = new ZookeeperLock(multiConfig);

        const expectedSuccess = [lock1.lock('test'), lock2.lock('test')];
        promise.all(expectedSuccess).then((results) => {
            return lock3.lock('test', 1000).then(() => {
                throw Error('should not have been able to lock');
            }).catch(ZookeeperLockTimeoutError, (err) => {
                expect(err.message).to.equal('timeout');
            }).catch((err) => {
                testComplete(err);
            });
        }).catch((err) => {
            testComplete(err)
        }).finally(() => {
            promise.all([lock1.unlock(), lock2.unlock(), lock3.destroy()]).finally(() => {
                testComplete();
            });
        });
        return null;
    });

    it("can fail immediately when already locked if configured as such", function (testComplete) {
        this.timeout(15000);
        const failImmediateConfig = {
            serverLocator: locator,
            pathPrefix: 'tests',
            sessionTimeout: 2000,
            failImmediate: true
        };
        const lock1 = new ZookeeperLock(failImmediateConfig);
        const lock2 = new ZookeeperLock(failImmediateConfig);
        const lock3 = new ZookeeperLock(failImmediateConfig);
        const lock4 = new ZookeeperLock(failImmediateConfig);
        const lock5 = new ZookeeperLock(failImmediateConfig);

        let failErr = null;
        lock1.lock('test').then(() => {
            return promise.all([lock2.lock('test'), lock3.lock('test'), lock4.lock('test'), lock5.lock('test')].map(function(p) {
                return p.reflect();
            }));
        }).each(function(inspection) {
            if (inspection.isFulfilled()) {
                throw Error('should not have been able to lock');
            } else {
                if (inspection.reason().message !== 'aborting lock process' && inspection.reason().message !== 'already locked') {
                    throw Error(`got wrong reason: ${inspection.reason().message}`);
                }
            }
        }).catch((err) => {
            expect(err).to.not.exist;
            failErr = err;
        }).finally(() => {
            promise.all([lock1.unlock(), lock2.destroy(), lock3.destroy(), lock4.destroy(), lock5.destroy()])
                .catch((err) => {
                    failErr = err;
                }).finally(() => {
                    testComplete(failErr);
                });
        });

        return null;
    });

    it("functions correctly when lockers ahead of it give up", (testComplete) => {
        /* testing code changes to use exists watchers instead of getChildren watcher, to ensure that lock3 is
         * able to lock correctly if lock2 (which it is watching for existence) gives up on waiting for lock1 to
         * finish, forcing lock3 to call getChildren again and watch existence on lock1
         */

        this.timeout(20000);
        const lockConfig = {
            serverLocator: locator,
            pathPrefix: 'tests',
            sessionTimeout: 2000
        };
        const lock1 = new ZookeeperLock(lockConfig);
        const lock2 = new ZookeeperLock(lockConfig);
        const lock3 = new ZookeeperLock(lockConfig);

        lock1.lock('test').then(() => {
            lock2.lock('test', 5000).catch(() => {
                lock1.unlock();
            });
            return promise.delay(100);
        }).then(() => {
            return lock3.lock('test');
        }).then(() => {
            testComplete();
        }).catch(testComplete);
        return;
    });
});

