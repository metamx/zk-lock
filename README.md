# zk-lock

Distributed locking using [node-zookeeper-client](https://github.com/alexguan/node-zookeeper-client), following the
recipe in the [zookeeper documentation](https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks).

The library exposes a single class, `ZookeeperLock` which is both the lock object, and provides static methods for 
initializing the configuration and acquiring locks which are already locked.

All locks are stored in a top level zookeeper folder `locks/`, can have an optional path prefix, and take a key argument
which indicates the resource that should be locked. 

Locks must be explicitly released with the `unlock` method in order to allow other processes to obtain the lock. `unlock` will also close the zookeeper connection.

## Configuration
`serverLocator: () => Promise<any>` serverLocator is a service discovery/location resolving library such as [locators](https://github.com/metamx/locators),
or anything which implements a function which takes no arguments and can return a promise of a `Location` object that has properties `host` and `port` which specify
where the zookeeper server can be discovered.

`pathPrefix: string` This is a prefix to add to any key which is to be locked. Note: all locks will 
start with the path `locks/`, so with a prefix of `foo/bar`, when locking the key `baz` the zookeeper 
path will be of the form `locks/foo/bar/baz`.

`sessionTimeout: number` node-zookeeper-client `sessionTimeout` parameter that is passed down to the zookeeper client.

`spinDelay: number` node-zookeeper-client `spinDelay` parameter that is passed down to the zookeeper client.

`retries: number` node-zookeeper-client `retries` parameter that is passed down to the zookeeper client.

	
## ZookeeperLock
### constructor
usage: 
```
	var lock = new ZookeeperLock(config);
  
```

### Static methods

#### initialize
usage: `ZookeeperLock.initialize(config);`

Initialize a global configuration for zookeeper locks.

#### lockFactory
usage: 
```
	var lock = ZookeeperLock.lockFactory();
```

#### lock
usage:
```
	ZookeeperLock.lock('key').then(function (lock) {
    	... do stuff
  	});
```

### Instance methods
#### lock
usage:
```
    var lock = ZookeeperLock.lockFactory();
    lock.lock('key').then(function() {
    	... do stuff
  	});
```

#### unlock
usage;
```
    var lock = ZookeeperLock.lockFactory();
    lock.lock('key').then(function() {
    	... do stuff
    	lock.unlock().then(function () {
    	    ... 
    	});
    });
```

### Session timeouts
Due to the nature of zookeeper, this locking strategy is susceptible to a situation when `sessionTimeout` occurs that a lock holder can still think that they own the lock, but the key has been lost on the zookeeper server. To handle this, `ZookeeperLock` exposes a `signal` property that emits a `lost` event when a `sessionTimeout` occurs, which MUST be handled by the lock holder to terminate execution.

usage:

```
    var lock = ZookeeperLock.lockFactory();
    lock.lock('key').then(function() {
    	lock.on('lost', function () {
    		... handle losing the lock, i.e. restart locking cycle.
    	});
    });
```


## Development
`npm run build` to compile.

The `ZookeeperBeacon` test currently require a local installation of zookeeper to run successfully, specified
by the `zkServerCommandPath` variable which is defined near the beginning of the tests.