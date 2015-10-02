# zs-rediz-lock

Generates read or write lockers for a zs-rediz client.

Has the following features: 

- Ability to lock a key or multpile keys for read or write purposes
- Store multiple locks and manipulate (upgrade, release) all at once
- Wrapper function to manipulate a locker then release
- Upgrade read locks to write locks

## Basic Usage

### Lock a Key
The example below shows how you can write lock a key, manipulate it, and then release it. The same is true for a read lock, just use the function `readLock`. 
readLock || writeLock options:
- maxWaitTime : This is the maxiumum amount of time in seconds to wait until the shard is unlocked. If it is set to 0, and the shard is not available it will return the error, otherwise it will keep trying until it locks or times out. This will default is 30 ceconds.
- lockTimout : This is the length of time in seconds before the lock expries. The default for this is 60 seconds.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);

locker.writeLock('key', { maxWaitTime: 30, lockTimeout: 60 }).then( (rwLock) => {
	.... do stuff with rwLock
}).then( (rwLock) => {
	return rwLock.release()
});
```

### Lock Upgrade
The example below shows how to easily turn a read lock into a write lock. It will unlock each key and then relock them with write keys.
Upgrade Options:
- maxWaitTime : This is the maxiumum amount of time in seconds to wait until the write shard is unlocked. If it is set to 0, and the shard is not available it will return the error, otherwise it will keep trying until it locks or times out. This will default is 30 ceconds.
- lockTimout : This is the length of time in seconds before the write lock expries. The default for this is 60 seconds.
- onError : This controls the process when an error is through. The default is `stop`, which immediately stops and throws an error. The other is `release` which will immedaitely release all the keys if an error occurs, no matter where in the process the error exists.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);

locker.readLock('key', { maxWaitTime: 30, lockTimeout: 60 }).then( (rwLock) => {
	return rwlock.upgrade({
		maxWaitTime: 30,
		lockTimeout: 60,
		onError: 'release'
	})
}).then( (rwLock) => {
	return rwLock.release()
});
```

### Lock Wrapping

This example shows you the `readLockWrap` and `writeLockWrap` functions. This allows you to run a function on key(s) and it releases them once the function is complete. It will return value from the function. Even if the function given returns or throws an error, the keys will be unlocked **before** returning.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);

locker.readLockWrap('key', () => {
	// read stuff here
	return returnValue;
}).then( (returnValue) => {
	...
}).catch( (error) => {
	...
});
```

### Lock Sets
This example shows how you can keep a set of locks. You can create a write or read lock directly on the set which will be added to the list of locks. When calling `lockSet.upgrade` or `lockSet.release`, all the locks will be upgraded or released in the inverese order they were originally locked. When a lock set is release all the locks are removed from the list of locks. When upgrading a lock set, only the read locks are released and then relocked as writes.

For the `lockSet.upgrade` the options agrument is the same for the locker upgrade with one exceptions. The `options.onError`, can be set to `ignore`. Which will ignore all errors on upgrade, no matter at what step the upgrade is in. It will then return an array of all failed upgrades. Note: a failed upgrade would not include a lock that is already a write lock.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);
let lockSet = locker.createLockSet();

lockSet.writeLock(['key', 'key1' ]).then( () => {
	return lockSet.readLock(['key2', 'key3']);
}).then( () => {
	return lockSet.upgrade({
		maxWaitTime: 30,
		lockTimeout: 60,
		onError: 'ignore'
	});
}).then( (upgradeFailures) => {
	if (upgradeFailures.length) {
		// do something here
	}
});
```