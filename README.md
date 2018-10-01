# rediz-lock

Generates read or write lockers for a rediz client.

Has the following features:

- Ability to lock a key or multpile keys for read or write purposes
- Store multiple locks and manipulate (upgrade, release) all at once
- Wrapper function to manipulate a locker then release
- Upgrade read locks to write locks

## Lock a Key
The example below shows how you can write lock a key, manipulate it, and then release it. The same is true for a read lock, just use the function `readLock`.
readLock || writeLock options:
- maxWaitTime : This is the maxiumum amount of time in seconds to wait until the shard is unlocked. If it is set to 0, and the shard is not available it will return the error, otherwise it will keep trying until it locks or times out. This will default is 30 ceconds.
- lockTimout : This is the length of time in seconds before the lock expries. The default for this is 60 seconds.
- resolveConflicts : __This option can only be supplied to `writeLock()`.__  If this is set to true, the following 
behavior is enabled: If we attempt to acquire a write lock, but the write lock is
already held by another process, we either immediately fail with a RESOURCE_LOCKED error
or we wait for the lock, depending on a conflict resolution process.  The "winner" of
the conflict resolution (the process that continues to wait for the lock) is randomly
chosen (but the same "winner" will be chosen by both).  Note that the process that
already holds the lock will continue to hold it, even if it loses conflict resolution.
It is allowed to run to completion and release the lock.  Essentially, if this is set to
true, it sometimes acts like `maxWaitTime` is set to 0 (essentially, randomly).

```js
let Locker = require('rediz-locker');
let Client = require('rediz');
let redizClient = new Client(config);

let locker = new Locker(redizClient);

locker.writeLock('key', { maxWaitTime: 30, lockTimeout: 60 }).then( (rwLock) => {
	.... do stuff with rwLock
}).then( (rwLock) => {
	return rwLock.release()
});
```

## Lock Upgrade
The example below shows how to easily turn a read lock into a write lock. It will unlock each key and then relock them with write keys.
Upgrade Options:
- maxWaitTime : This is the maxiumum amount of time in seconds to wait until the write shard is unlocked. If it is set to 0, and the shard is not available it will return the error, otherwise it will keep trying until it locks or times out. This will default is 30 ceconds.
- lockTimout : This is the length of time in seconds before the write lock expries. The default for this is 60 seconds.
- onError : This controls the process when an error is through. The default is `stop`, which immediately stops and throws an error. The other is `release` which will immedaitely release all the keys if an error occurs, no matter where in the process the error exists.

```js
let Locker = require('rediz-locker');
let Client = require('rediz');
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

## Lock Wrapping

This example shows you the `readLockWrap` and `writeLockWrap` functions. This allows you to run a function on key and it releases them once the function is complete. It will return value from the function. Even if the function given returns or throws an error, the keys will be unlocked **before** returning.

```js
let Locker = require('rediz-locker');
let Client = require('rediz');
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

## Lock Sets

Rediz-Lock also provides a LockSet class that manages a set of locks together.

### Creating

To create an empty lock set:

```js
let lockSet = locker.createLockSet();
```

Or, to create a lock set that already has keys locked:

```js
let lockSet = locker.readLockSet([ 'key1', 'key2' ]);
let otherLockSet = locker.writeLockSet([ 'key1', 'key2' ]);
```

Note that, although these examples use a LockSet with only read locks or only write locks,
LockSets can contain mixed read/write locks.

### Adding Locks

Use methods on the LockSet to add new locks.  The available methods are the same as those on
`Locker`:

- readLock()
- writeLock()
- readLockWrap()
- writeLockWrap()

Locks returned from these methods can be individually released, upgraded, and managed if needed.

### Reference Counting

If a LockSet is requested to lock the same key twice, it will return the same lock object and
increment a reference counter.  So, if the key `key1` is locked twice, you will need to call
`release()` twice to actually release the lock.

### Releasing & Upgrading

The LockSet also has methods to release or upgrade the whole LockSet at once:

```js
// Releases all locks in the set
lockSet.release()

// Upgrades all locks in the set
lockSet.upgrade()

// Releases all locks even if they have nonzero reference counts
lockSet.forceRelease()
```

### Dependent LockSets

You can also add a whole LockSet to another LockSet as a dependency.  This allows you to refer to
nested sets of locks.

```js
// Create an empty dependent lock set
let lockSet = locker.createLockSet();
let dependentLockSet = lockSet.createLockSet();
dependentLockSet.writeLock(...);

// Create dependent lock sets with keys already locked
lockSet.readLockSet([ 'key1', 'key2', ... ]) // -> LockSet
lockSet.writeLockSet([ 'key1', 'key2', ... ]) // -> LockSet
```

## Distributed Reader/Writer Locks

In workloads where there will be many concurrent readers (enough to overwhelm a single redis shard)
but infrequent writers, "distributed" locks can be used.  These can be enabled by passing
`{ distributed: true }` to the options of `readLock()` and `writeLock()`.  A distributed read lock
selects a randomized shard to store the read lock instead of a consistent shard assigned by key.  This
allows the load of reader locks to be distributed across multiple shards.  A distributed write lock
is locked on every available shard.

It is also possible for `writeLock()` to automatically detect whether a distributed read lock has been
recently established on a key, and automatically choose between a distributed lock and a normal lock.
To enable this behavior, pass `{ distributed: 'auto' }` to `writeLock()`.  By default, `readLock()`
will set additional flags when operating in distributed mode to allow `writeLock()` to detect this.
If not using this automatic functionality, this behavior in `readLock()` can be disabled for added
efficiency by passing `{ enableDistributedAuto: false }`.

## Conflict Resolution Locks

This type of write lock can be helpful in cases to prevent deadlocks while guaranteeing forward
progress in distributed workloads.  It is generally used where normal deadlock prevention algorithms
won't work or are insufficient.

If the `{ resolveConflicts: true }` option is given to `writeLock()`, it enables special behavior in
the case that the lock is already held by another process.  If another process already holds the lock,
then a "winner" is chosen between the current process and the existing lock holder.  If the current
process is the winner, the write lock behaves normally (it waits for the other process to release the lock,
then obtains the lock itself).  If the current process is the loser, then `writeLock()` immediately fails
with a `RESOURCE_LOCKED` error.  (In the latter case, the application would typically release all other
locks it holds, then retry the operation after a delay.)

Generally, the winner is chosen randomly between the two competing processes.  This can be tweaked by
supplying the `conflictPriority` option.  This option is a number from 0-99 and defaults to 50.  If two
processes conflict with different priorities, then the one with the lowest number wll be the winner.
