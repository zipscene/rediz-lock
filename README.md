# zs-rediz-lock

Generates read or write lockers for a zs-rediz client.

Has the following features: 

- Ability to lock a key or multpile keys for read or write purposes
- Store multiple locks and manipulate (upgrade, release) all at once
- Wrapper function to manipulate a locker then release
- Upgrade read locks to write locks

## Basic Usage

The example below shows how you can write lock a key, manipulate it, and then release it. The same is true for a read lock, just use the function `readLock`. 
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

This example shows how you can keep a set of locks. You can create a write or read lock directly on the set which will be added to the list of locks. When calling `lockSet.upgrade` or `lockSet.release`, all the locks will be upgraded or released in the inverese order they were originally locked.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);
let lockSet = locker.createLockSet();

lockSet.writeLock(['key', 'key1' ]).then( () => {
	return lockSet.readLock(['key2', 'key3']);
}).then( () => {
	lockSet.release();
})
```

This example shows you the `readLockWrap` and `writeLockWrap` functions. This allows you to run a function on key(s) and it releases them once the function is complete. It will return value from the function. Even if the function given returns or throws an error, the keys will be unlocked **before** returning.

```js
let Locker = require('rediz-locker');
let Client = require('zs-rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);

locker.readLockWrap('key', doSomethingCrazy()).then( (returnValue) => {
	...
}).catch( (error) => {
	...
});
```