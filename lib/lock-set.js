const pasync = require('pasync');
const _ = require('lodash');
const XError = require('xerror');
/**
 * Contains a collection of locks that can all be released or upgraded at once.
 *
 * @class LockSet
 */
class LockSet {

	/**
	 * @constructor
	 * @param {locker} locker - A locker instance.
	 */
	constructor(locker) {
		this.locks = {};
		this.locker = locker;
	}

	/**
	 * Adds a lock to the collection.
	 *
	 * @method addLock
	 * @throws XError
	 */
	addLock(lock) {
		let keys = lock.keys || [];
		for (let key of keys) {
			if (this.locks[key]) {
				throw new XError(XError.INTERNAL_ERROR, 'A lock is already held for this key');
			}
			this.locks[key] = lock;
		}
	}

	/**
	 * Returns the lock this LockSet holds on the given key, or undefined if no such lock exists.
	 *
	 * @method getLock
	 * @return {RWLock}
	 */
	getLock(key) {
		return this.locks[key];
	}

	/**
	 * Returns true if this LockSet holds any locks, and false otherwise.
	 *
	 * @method _hasLocks
	 * @protected
	 * @return {Boolean}
	 */
	_hasLocks() {
		return Object.keys(this.locks).length !== 0;
	}

	/**
	 * Releases all locks in the collection.
	 *
	 * @method release
	 * @return {Promise}
	 */
	release() {
		return pasync.eachSeries(Object.keys(this.locks).reverse(), (lockKey) => {
			let lock = this.locks[lockKey];
			return lock.release();
		}).then( () => {
			this.locks = [];
		});
	}

	/**
	 * Upgrades all read locks by releasing the write lock and then relocking them with a read lock
	 *
	 * @method upgrade
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout=60]
	 *   @param {Number} [options.maxWaitTime=30]
	 *	 @param {String} [options.onError='stop'] - This control how to handle when an error occurs on upgrade.
	 * 		There are 3 different settings for this options.
	 *		[1] stop - this will error out if one of the locks errors on upgrade and throw the error.
	 *		[2] release - will release all locks (including write locks) that are in the set.
	 *		[3] ignore - ignore all errors and continue upgrading the rest,
	 *		the return value will be an array of all locks that could not be upgraded.
	 * @return {Promise}
	 */
	upgrade(options = {}) {
		let lockTimeout = options.lockTimeout || 60;
		let maxWaitTime = options.maxWaitTime || 30;
		let onErr = options.onError || 'stop';
		let upgradeFails = [];
		return pasync.eachSeries(Object.keys(this.locks).reverse(), (lockKey) => {
			let lock = this.locks[lockKey];
			return lock.upgrade({ maxWaitTime, lockTimeout }).catch( (error) => {
				if (onErr === 'ignore') {
					upgradeFails.push(lock);
					return null;
				} else {
					throw error;
				}
			});
		}).then( () => {
			if (upgradeFails.length) {
				return upgradeFails;
			}
		}).catch( (error) => {
			if (onErr === 'release') {
				return this.release().then( () => {
					throw error;
				});
			} else {
				throw error;
			}
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
			this.addLock(rwLock);
		});
	}

}

module.exports = LockSet;
