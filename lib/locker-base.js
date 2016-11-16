// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const pasync = require('pasync');
const _ = require('lodash');
const XError = require('xerror');

/**
 * Base class for Locker and LockSet.  Contains common code shared between the two of them.
 *
 * @class LockerBase
 */
class LockerBase {

	/**
	 * Creates a new lock set.
	 *
	 * @method createLockSet
	 * @return {LockSet}
	 */
	createLockSet() { }

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
	 * @param {String} key - The key to read lock on
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout] - Amount of time, in seconds, the lock can remain
	 *     locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	 *     cleared.
	 *   @param {Number} [options.maxWaitTime] - Maximum amount of time, in seconds, to wait for
	 *     the lock to become available.  If this is 0, this function returns immediately if the
	 *     lock cannot be acquired.
	 * @return {Promise{RWLock}} - Resolves with the RWLock instance which is used to release (or
	 *   upgrade) the lock instance.  Rejects with an XError.  If the lock cannot be acquired
	 *   because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError` (an XError
	 *   with a code of `XError.RESOURCE_LOCKED`).
	 */
	readLock(/* key, options = {} */) { }

	/**
	 * Acquires a writer lock on a key.  As long as any thread has a writer lock on a key, no other
	 * threads can have either a reader or a writer lock.
	 *
	 * @method writeLock
	 * @param {String} key
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout]
	 *   @param {Number} [options.maxWaitTime]
	 * @return {Promise{RWLock}}
	 */
	writeLock(/* key, options = {} */) { }

	/**
	* Acquires a read lock for each key given. If any of the keys fail to be locked, all
	* that were in the list that were locked will be released, and the error will be returned.
	*
	* @method readLockSet
	* @param {String[]} keys - the keys to read lock on
	* @param {Object} [options={}]
	*   @param {Number} [options.lockTimeout] - Amount of time, in seconds, the lock can remain
	*     locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	*     cleared.
	*   @param {Number} [options.maxWaitTime] - Maximum amount of time, in seconds, to wait for
	*     the lock to become available.  If this is 0, this function returns immediately if the
	*     lock cannot be acquired.
	*   @param {LockSet} [options.lockSet] - a lockSet you would like these new locks to be added to
	* @return {Promise{LockSet}} - Resolves with a new lockSet containing all locks that were read locked,
	*  or the lockSet given with the new read locks added to the set. Rejects with an XError. If the lock
	*  cannot be acquired because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError`
	*  (an XError with a code of `XError.RESOURCE_LOCKED`).
	*/

	readLockSet(keys, options = {}) {
		return this._rwLockSet(keys, false, options);
	}

	/**
	* Acquires a write lock for each key given. If any of the keys fail to be locked, all
	* that were in the list that were locked will be released, and the error will be returned.
	*
	* @method writeLockSet
	* @param {String[]} keys - the keys to read lock on
	* @param {Object} [options={}]
	*   @param {Number} [options.lockTimeout] - Amount of time, in seconds, the lock can remain
	*     locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	*     cleared.
	*   @param {Number} [options.maxWaitTime] - Maximum amount of time, in seconds, to wait for
	*     the lock to become available.  If this is 0, this function returns immediately if the
	*     lock cannot be acquired.
	*   @param {LockSet} [options.lockSet] - a lockSet you would like these new locks to be added to
	* @return {Promise{LockSet}} - Resolves with a new lockSet containing all locks that were read locked,
	*  or the lockSet given with the new read locks added to the set. Rejects with an XError. If the lock
	*  cannot be acquired because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError`
	*  (an XError with a code of `XError.RESOURCE_LOCKED`).
	*/
	writeLockSet(keys, options = {}) {
		return this._rwLockSet(keys, true, options);
	}

	/**
	* Acquires a write lock for each key given. If any of the keys fail to be locked, all
	* that were in the list that were locked will be released, and the error will be returned.
	*
	* @private
	* @method _rwLockSet
	* @param {String[]} keys - the keys to read lock on
	* @param {Boolean} toWrite - flag to know whether or not it should be a write lock or a read lock
	* @param {Object} [options={}]
	*   @param {Number} [options.lockTimeout] - Amount of time, in seconds, the lock can remain
	*     locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	*     cleared.
	*   @param {Number} [options.maxWaitTime] - Maximum amount of time, in seconds, to wait for
	*     the lock to become available.  If this is 0, this function returns immediately if the
	*     lock cannot be acquired.
	*   @param {LockSet} [options.lockSet] - a lockSet you would like these new locks to be added to
	* @return {Promise{LockSet}} - Resolves with a new lockSet containing all locks that were read locked,
	*  or the lockSet given with the new read locks added to the set. Rejects with an XError. If the lock
	*  cannot be acquired because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError`
	*  (an XError with a code of `XError.RESOURCE_LOCKED`).
	*/
	_rwLockSet(keys, toWrite, options) {
		if (!_.isArray(keys)) return Promise.reject(new XError(XError.INVALID_ARGUMENT, 'keys must be an array'));
		let locks = [];
		let lockSet = options.lockSet ? options.lockSet : this.createLockSet();
		let func = toWrite ? this.writeLock : this.readLock;
		return pasync.eachSeries(keys, (key) => {
			if (lockSet.getLock(key)) return Promise.resolve();
			if (_.includes(_.pluck(locks, 'key'), key)) return Promise.resolve();
			return func.call(this, key, options)
			.then( (lock) => locks.push(lock));
		})
		.then( () => {
			if (!locks.length) return lockSet;
			for (let lock of locks) {
				lockSet.addLock(lock);
			}
			return lockSet;
		})
		.catch( (error) => {
			return pasync.eachSeries(locks, (lock) => {
				return lock.release();
			})
			.then( () => {
				throw error;
			});
		});
	}

	/**
	 * Wraps a function in a read lock.  The read lock is acquired before the function is executed,
	 * and is released when the function returns, regardless of whether or not it errors.  The
	 * function can be synchronous, or it can return a Promise.  If it returns a Promise, the
	 * resolve/reject values are forwarded to the result of `readLockWrap()` .
	 *
	 * @method readLockWrap
	 * @param {String} key
	 * @param {Object} [options={}]
	 * @param {Function} fn - Function that is executed when the lock is acquired.
	 * @return {Promise} - Resolves or rejects with the return value of `fn` .
	 */
	readLockWrap(key, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		let rwlock;
		let handleError = (error) => {
			if (rwlock) {
				return rwlock.release().then( () => {
					throw error;
				});
			} else {
				throw error;
			}
		};
		return this.readLock(key, options)
			.then( (_rwlock) => {
				rwlock = _rwlock;
				return fn(rwlock);
			})
			.then( (result) => {
				if (rwlock.isLocked) {
					return rwlock.release().then( () => {
						return result;
					});
				} else {
					return result;
				}
			}, (error) => {
				return handleError(error);
			})
			.catch( (error) => {
				return handleError(error);
			});
	}

	/**
	 * Wraps a function in a write lock.
	 *
	 * @method writeLockWrap
	 * @param {String} key
	 * @param {Object} [options={}]
	 * @param {Function} fn
	 * @return {Promise}
	 */
	writeLockWrap(key, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		let rwlock;
		let handleError = (error) => {
			if (rwlock) {
				return rwlock.release().then( () => {
					throw error;
				});
			} else {
				throw error;
			}
		};
		return this.writeLock(key, options)
			.then( (_rwlock) => {
				rwlock = _rwlock;
				return fn();
			})
			.then( (result) => {
				return rwlock.release().then( () => {
					return result;
				});
			}, (error) => {
				return handleError(error);
			})
			.catch( (error) => {
				return handleError(error);
			});
	}

}

module.exports = LockerBase;
