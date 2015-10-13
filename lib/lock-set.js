const pasync = require('pasync');
const ResourceLockedError = require('./resource-locked-error');
/**
 * Contains a collection of locks that can all be released or upgraded at once.
 *
 * @class LockSet
 */
class LockSet {

	/**
	 * @constructor
	 */
	constructor() {
		this.locks = {};
		this.dependentLockSets = [];
	}

	/**
	 * Adds a lock to the collection.
	 *
	 * @method addLock
	 * @param lock - the lock to add to the set
	 * @throws XError
	 */
	addLock(lock) {
		if (this.locks[lock.key]) throw new ResourceLockedError(key, 'A lock is already held for this key');
		this.locks[lock.key] = lock;
	}

	/**
	 * Add a dependent LockSet to this LockSet. If this LockSet's release() method is called, all
	 * dependent LockSets will also be released.
	 *
	 * @method addDependentLockSet
	 * @param {LockSet} lockSet
	 */
	addDependentLockSet(lockSet) {
		this.dependentLockSets.push(lockSet);
	}

	/**
	 * Returns the lock this LockSet holds on the given key, or undefined if no such lock exists.
	 *
	 * @method getLock
	 * @param {String} key
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
		if (Object.keys(this.locks).length > 0) return true;
		for (let lockSet of this.dependentLockSets) {
			if (lockSet._hasLocks()) return true;
		}
		return false;
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
			return pasync.eachSeries(this.dependentLockSets.reverse(), (lockSet) => {
				return lockSet.release();
			}).then( () => {
				this.dependentLockSets = [];
			});
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

}

module.exports = LockSet;
