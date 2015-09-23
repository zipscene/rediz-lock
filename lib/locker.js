
/**
 * Main class that acquires locks.
 *
 * @class Locker
 */
class Locker {

	/**
	 * @constructor
	 * @param {RedizClient} redizClient - The RedizClient class to use to access redis.
	 * @param {Object} [options]
	 *   @param {String} [options.prefix='rzlock:'] - Optional redis key prefix
	 */
	constructor(redizClient, options = {}) {
		// In the constructor, read in the lua script files and register them with redizClient

	}

	/**
	 * Creates a new lock set.
	 *
	 * @method createLockSet
	 * @return {LockSet}
	 */
	createLockSet() {
		return new LockSet(this);
	}

	/**
	 * Alias of `writeLock()`
	 *
	 * @method lock
	 */
	lock(key, options) {
		return this.writeLock(key, options);
	}

	/**
	 * Acquires a reader lock on a key.  Multiple threads may have reader locks on the same key,
	 * but no thread may have a reader lock if any thread has a writer lock.
	 *
	 * @method readLock
	 * @param {String|String[]} key - The string key to lock on, or an array of keys to
	 *   lock (in order).
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout=60] - Amount of time, in seconds, the lock can remain
	 *     locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	 *     cleared.
	 *   @param {Number} [options.maxWaitTime=30] - Maximum amount of time, in seconds, to wait for
	 *     the lock to become available.  If this is 0, this function returns immediately if the
	 *     lock cannot be acquired.
	 * @return {Promise{RWLock}} - Resolves with the RWLock instance which is used to release (or
	 *   upgrade) the lock instance.  Rejects with an XError.  If the lock cannot be acquired
	 *   because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError` (an XError
	 *   with a code of `XError.RESOURCE_LOCKED`).
	 */
	readLock(key, options = {}) {
		if (!_.isArray(key)) { key = [ key ]; }

	}

	/**
	 * Acquires a writer lock on a key.  As long as any thread has a writer lock on a key, no other
	 * threads can have either a reader or a writer lock.
	 *
	 * @method writeLock
	 * @param {String|String[]} key
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout=60]
	 *   @param {Number} [options.maxWaitTime=30]
	 * @return {Promise{RWLock}}
	 */
	writeLock(key, options = {}) {
		if (!_.isArray(key)) { key = [ key ]; }

	}

	/**
	 * Wraps a function in a read lock.  The read lock is acquired before the function is executed,
	 * and is released when the function returns, regardless of whether or not it errors.  The
	 * function can be synchronous, or it can return a Promise.  If it returns a Promise, the
	 * resolve/reject values are forwarded to the result of `readLockWrap()` .
	 *
	 * @method readLockWrap
	 * @param {String|String[]} key
	 * @param {Object} [options={}]
	 * @param {Function} fn - Function that is executed when the lock is acquired.
	 * @return {Promise} - Resolves or rejects with the return value of `fn` .
	 */
	readLockWrap(key, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		if (!_.isArray(key)) { key = [ key ]; }

	}

	/**
	 * Wraps a function in a write lock.
	 *
	 * @method writeLockWrap
	 * @param {String|String[]} key
	 * @param {Object} [options={}]
	 * @param {Function} fn
	 * @return {Promise}
	 */
	writeLockWrap(key, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		if (!_.isArray(key)) { key = [ key ]; }

	}

}

module.exports = Locker;
