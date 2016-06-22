const uuid = require('node-uuid');
const pasync = require('pasync');
const Profiler = require('zs-simple-profiler');
const ResourceLockedError = require('./resource-locked-error');
const LockerBase = require('./locker-base');
const _ = require('lodash');

const profiler = new Profiler('Locker');

/**
 * Contains a collection of locks that can all be released or upgraded at once.
 *
 * @class LockSet
 * @constructor
 * @param {Locker} locker - The locker that created this LockSet
 */
class LockSet extends LockerBase {

	constructor(locker) {
		super();
		this.locks = {};
		this.dependentLockSets = [];
		this.locker = locker;
		this.tokenBase = uuid.v4().substr(0, 17);
	}

	/**
	 * Adds a lock to the collection.
	 *
	 * @method addLock
	 * @param lock - the lock to add to the set
	 * @throws XError
	 */
	addLock(lock) {
		if (this.locks[lock.key]) throw new ResourceLockedError(lock.key, 'A lock is already held for this key');
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
	 * Forces releasing all locks in the collection, regardless of reference counts.
	 *
	 * @method forceRelease
	 * @return {Promise}
	 */
	forceRelease() {
		return pasync.eachSeries(Object.keys(this.locks).reverse(), (lockKey) => {
			let lock = this.locks[lockKey];
			return lock.forceRelease();
		}).then( () => {
			this.locks = [];
			return pasync.eachSeries(this.dependentLockSets.reverse(), (lockSet) => {
				return lockSet.forceRelease();
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
	 *   @param {Number} [options.lockTimeout]
	 *   @param {Number} [options.maxWaitTime]
	 *	 @param {String} [options.onError='stop'] - This control how to handle when an error occurs on upgrade.
	 * 		There are 3 different settings for this options.
	 *		[1] stop - this will error out if one of the locks errors on upgrade and throw the error.
	 *		[2] release - will release all locks (including write locks) that are in the set.
	 *		[3] ignore - ignore all errors and continue upgrading the rest,
	 *		the return value will be an array of all locks that could not be upgraded.
	 * @return {Promise}
	 */
	upgrade(options = {}) {
		let { lockTimeout, maxWaitTime } = _.defaults(options, this.locker.defaults);

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
		})
			.then(() => {
				if (upgradeFails.length) {
					return upgradeFails;
				}
			})
			.catch((error) => {
				if (onErr === 'release') {
					return this.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
	}

	/**
	 * Acquires a writer lock on a key.  As long as any thread has a writer lock on a key, no other
	 * threads can have either a reader or a writer lock.  If this LockSet already contains a lock
	 * on the given key, its reference count is updated instead, and it can be released multiple
	 * times.
	 *
	 * @method writeLock
	 * @param {String} key
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout]
	 *   @param {Number} [options.maxWaitTime]
	 *   @param {String} [options.tokenBase]
	 * @return {Promise{RWLock}}
	 */
	writeLock(key, options = {}) {
		let prof = profiler.begin('#writeLock');

		if (!options.tokenBase) options.tokenBase = this.tokenBase;
		if (this.locks[key]) {
			return this.locks[key].upgrade(options)
				.then((lock) => {
					lock._relock();
					return lock;
				})
				.then(prof.wrappedEnd());
		} else {
			return this.locker.writeLock(key, options)
				.then((lock) => {
					this.addLock(lock);
					return lock;
				})
				.then(prof.wrappedEnd());
		}
	}

	/**
	 * Acquires a reader lock on a key.  Multiple threads may have reader locks on the same key,
	 * but no thread may have a reader lock if any thread has a writer lock.  If this LockSet
	 * already contains the given key, its reference count is updated instead.
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
	readLock(key, options = {}) {
		let prof = profiler.begin('#readLock');

		if (this.locks[key]) {
			return Promise.resolve(this.locks[key]._relock())
				.then(prof.wrappedEnd());
		} else {
			return this.locker.readLock(key, options)
				.then((lock) => {
					this.addLock(lock);
					return lock;
				})
				.then(prof.wrappedEnd());
		}
	}

	/**
	 * Creates a new lock set and adds it as a dependent LockSet.
	 *
	 * @method createLockSet
	 * @return {LockSet}
	 */
	createLockSet() {
		let dependentSet = this.locker.createLockSet();
		this.addDependentLockSet(dependentSet);
		return dependentSet;
	}

}

module.exports = LockSet;
