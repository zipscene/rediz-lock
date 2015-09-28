const pasync = require('pasync');
/**
 * Contains a collection of locks that can all be released or upgraded at once.
 *
 * @class LockSet
 */
class LockSet {

	constructor(locker) {
		this.locks = [];
		this.locker = locker;
	}

	/**
	 * Adds a lock to the collection.
	 *
	 * @method addLock
	 */
	addLock(lock) {
		return this.locks.push(lock);
	}

	/**
	 * Releases all locks in the collection.
	 *
	 * @method release
	 * @return {Promise}
	 */
	release() {
		return pasync.eachSeries(this.locks.reverse(), (lock) => {
			return lock.release();
		});
	}

	/**
	 * Upgrades all locks in the collection.
	 *
	 * @method upgrade
	 * @return {Promise}
	 */
	upgrade() {
		return pasync.eachSeries(this.locks.reverse(), (lock) => {
			return lock.release();
		});
	}

	///////// EACH OF THESE BELOW CALLS THE CORRESPONDING FUNCTION ON `this.locker` AND
	///////// AUTOMAGICALLY ADDS THE RESULT TO THIS LOCK SET.

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
		return this.locker.readLock(key, options).then( (rwLock) => {
			return this.addLock(rwLock);
		});
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
		return this.locker.writeLock(key, options).then( (rwLock) => {
			return this.addLock(rwLock);
		});
	}

}

module.exports = LockSet;
