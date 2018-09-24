// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const XError = require('xerror');
const pasync = require('pasync');
/**
 * A class with the same interface as RWLock that represents a distributed write lock.
 *
 * @class DistributedWriteLock
 */
class DistributedWriteLock {

	/**
	 * @constructor
	 * @param {Locker} locker - The `Locker` instance that created this `RWLock` .
	 * @param {String} key - The keys that are locked by this RWLock.
	 * @param {String} token - The token that is used for write lock key.
	 * @param {Mixed} distributedShard - null for a normal lock, shard number for a distributed read lock
	 * @param {Boolean} [isWriteLock=false] - If true, this lock is a write lock.
	 * @param {Number} [heartbeatInterval=1000] - Heartbeat interval in milliseconds, null or false to disable
	 * @param {Number} [heartbeatTimeout=5] - Heartbeat timeout in seconds
	 */
	constructor(rwlocks) {
		this.rwlocks = rwlocks;
		this.locker = rwlocks[0].locker;
		this.key = rwlocks[0].key;
		this.isWriteLock = rwlocks[0].isWriteLock;
		this.isLocked = true;
		this.token = rwlocks[0].token;
		// Number of times this lock has been locked.  Expect the same number of releases.
		this.referenceCount = 1;
	}

	/**
	 * Forces releasing the lock immediately, regardless of reference counts.  This does not
	 * decrement the reference count.
	 *
	 * @return {Promise} - Resolves when the lock is released.
	 */
	async forceRelease() {
		if (!this.isLocked) return;
		this.isLocked = false;
		let promises = [];
		for (let lock of this.rwlocks) {
			promises.push(lock.forceRelease());
		}
		await Promise.all(promises);
	}

	/**
	 * Decrements the reference counter.  If it is decremented to zero, the lock is released.
	 *
	 * @return {Promise} - Resolves when lock is released or reference counter is decremented.
	 */
	release() {
		this.referenceCount--;
		if (this.referenceCount < 0) {
			this.referenceCount = 0;
			console.warn('Lock on ' + this.key + ' released too many times');
		}
		if (this.referenceCount === 0) {
			return this.forceRelease();
		} else {
			return Promise.resolve();
		}
	}

	/**
	 * Increments the reference counter of this lock.  Throws an error if the lock is already
	 * released.
	 *
	 * @method _relock
	 * @protected
	 * @throws {XError}
	 * @return {RWLock} - this
	 */
	_relock() {
		if (!this.isLocked) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot relock a lock after release');
		}
		this.referenceCount++;
		return this;
	}

}

module.exports = DistributedWriteLock;
