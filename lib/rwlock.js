// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const XError = require('xerror');
const pasync = require('pasync');
/**
 * A class representing a reader/writer lock on one or more keys.
 *
 * @class RWLock
 */
class RWLock {

	/**
	 * @constructor
	 * @param {Locker} locker - The `Locker` instance that created this `RWLock` .
	 * @param {String} key - The keys that are locked by this RWLock.
	 * @param {String} token - The token that is used for write lock key.
	 * @param {Boolean} [isWriteLock=false] - If true, this lock is a write lock.
	 * @param {Number} [heartbeatInterval=1000] - Heartbeat interval in milliseconds, null or false to disable
	 * @param {Number} [heartbeatTimeout=5] - Heartbeat timeout in seconds
	 */
	constructor(locker, key, token, isWriteLock = false, heartbeatInterval = 1000, heartbeatTimeout = 5) {
		this.locker = locker;
		this.key = key;
		this.isWriteLock = isWriteLock;
		this.isLocked = true;
		this.token = token;
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatTimeout = heartbeatTimeout;
		// Number of times this lock has been locked.  Expect the same number of releases.
		this.referenceCount = 1;
		if (this.isWriteLock && !this.token) {
			throw new XError(XError.INTERNAL_ERROR, 'This should have the same number of tokens as keys ' +
				'if it is a write lock');
		}
		this._startHeartbeat();
	}

	_startHeartbeat() {
		if (
			typeof this.heartbeatInterval !== 'number' ||
			typeof this.heartbeatTimeout !== 'number' ||
			this.heartbeatInterval <= 0 ||
			this.heartbeatTimeout <= 0 ||
			this.heartbeatHandle
		) {
			return;
		}
		this.heartbeatHandle = setInterval(() => {
			let client = this.locker.redizClient.shard(this.key);
			let promise;
			if (this.isWriteLock) {
				promise = client.runScript(
					'writeLockHeartbeat',
					this.locker.prefix + ':write:' + this.key,
					this.token,
					this.heartbeatTimeout
				);
			} else {
				promise = client.runScript(
					'readLockHeartbeat',
					this.locker.prefix + ':read:' + this.key,
					this.token,
					this.heartbeatTimeout
				);
			}
			promise.then((result) => {
				if (result[0] === 0) {
					console.warn('Tried to heartbeat lock not owned by this locker.  Owned by: ' + result[1]);
					this._stopHeartbeat();
				} else if (result[0] === 3) {
					console.warn('Lock expired before heartbeat: ' + this.key);
					this._stopHeartbeat();
				} else if (result[0] !== 1) {
					console.warn('Lock heartbeat failed: ' + result[0]);
					this._stopHeartbeat();
				}
			}).catch(pasync.abort);
		}, this.heartbeatInterval);
	}

	_stopHeartbeat() {
		if (this.heartbeatHandle !== undefined) {
			clearInterval(this.heartbeatHandle);
			delete this.heartbeatHandle;
		}
	}

	/**
	 * Forces releasing the lock immediately, regardless of reference counts.  This does not
	 * decrement the reference count.
	 *
	 * @return {Promise} - Resolves when the lock is released.
	 */
	forceRelease() {
		this._stopHeartbeat();
		if (!this.isLocked) return Promise.resolve();
		this.isLocked = false;
		if (!this.isWriteLock) {
			let client = this.locker.redizClient.shard(this.key, { downNodeExpiry: 0 });
			return client.runScript('readLockRelease', this.locker.prefix + ':read:' + this.key, this.token);
		} else {
			let client = this.locker.redizClient.shard(this.key, { downNodeExpiry: 0 });
			return client.runScript('writeLockRelease', this.locker.prefix + ':write:' + this.key, this.token)
				.catch( (error) => {
					if (error.message !== 'Shard unavailable' && error.code === 'redis_error') {
						console.warn(error);
						return null;
					} else {
						throw error;
					}
				});
		}
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
	 * Upgrades a reader lock to a writer lock.
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout]
	 *   @param {Number} [options.maxWaitTime]
	 * 	 @param {String} [options.onError=stop] - This setting is to determine the functionality on errors.
	 *		The default is stop, which will immediately hault the process and send back the error. The other setting
	 *		is release, which will immediately release all the keys when an error returns.
	 *   @param {String} [options.tokenBase]
	 *
	 * @return {Promise{RWLock}} - Resolves with `this`
	 */
	upgrade(options = {}) {
		if (!this.isLocked) {
			return Promise.reject(new XError(XError.INTERNAL_ERROR, 'Cannot upgrade a lock that has been released.'));
		}
		if (this.isWriteLock) return Promise.resolve(this);
		let onErr = options.onError || 'stop';
		return this.forceRelease().then( () => {
			return this.locker.writeLock(this.key, options);
		}).then( (newLock) => {
			this.token = newLock.token;
			this.isWriteLock = true;
			this.isLocked = true;
			newLock._stopHeartbeat();
			this._startHeartbeat();
			return this;
		}).catch( (error) => {
			if (onErr === 'release') {
				return this.forceRelease().then( () => {
					throw error;
				});
			} else {
				throw error;
			}
		});
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

module.exports = RWLock;
