const pasync = require('pasync');
const XError = require('xerror');
/**
 * A class representing a reader/writer lock on one or more keys.
 *
 * @class RWLock
 */
class RWLock {

	/**
	 * @constructor
	 * @param {Locker} locker - The `Locker` instance that created this `RWLock` .
	 * @param {String[]} keys - The keys that are locked by this RWLock.
	 * @param {String[]} [tokens=[]] - The tokens that are used for write locks.
	 * @param {Boolean} [isWriteLock=false] - If true, this lock is a write lock.
	 */
	constructor(locker, keys, tokens = [], isWriteLock = false) {
		this.locker = locker;
		this.keys = keys;
		this.isWriteLock = isWriteLock;
		this.isLocked = true;
		this.tokens = tokens;
		if (this.isWriteLock && this.keys.length !== this.tokens.length) {
			throw new XError(XError.INTERNAL_ERROR, 'This should have a the same number of tokens as keys ' +
				'if it is a write lock');
		}
	}

	/**
	 * Releases all the locks held by this RWLock.
	 *
	 * @return {Promise} - Resolves when locks are released.
	 */
	release() {
		if (!this.isLocked) return Promise.resolve();
		if (!this.isWriteLock) {
			return pasync.eachSeries(this.keys.reverse(), (key) => {
				let client = this.locker.redizClient.shard(key, { downNodeExpiry: 0 });
				client.runScript('readLockRelease', this.locker.prefix + ':read:' + key);
			}).then( () => {
				this.isLocked = false;
			});
		} else {
			let count = -1;
			let tokens = this.tokens.reverse();
			return pasync.eachSeries(this.keys.reverse(), (key) => {
				count = count + 1;
				console.log(count);
				let client = this.locker.redizClient.shard(key, { downNodeExpiry: 0 });
				return client.runScript('writeLockRelease', this.locker.prefix + ':write:' + key, tokens[count]);
			}).then( () => {
				this.isLocked = false;
			}).catch( (error) => {
				if (error.message !== 'Shard unavailable' && error.code === 'redis_error') {
					console.warn(error);
					return this.isLocked = false;
				} else {
					throw error;
				}
			});
		}
	}

	/**
	 * Upgrades a reader lock to a writer lock.
	 * @param {Object} [options={}]
	 * 		@param {Number} [lockTimeout=60] - Amount of time, in seconds, the lock can remain
	 *		locked.  After this timeout, the lock is assumed to be abandoned and is automatically
	 *		cleared.
	 * @return {Promise}
	 */
	upgrade(options) {
		if (!this.isLocked) {
			return Promise.reject(new XError(XError.INTERNAL_ERROR,
		'Cannot upgrade a lock that has been released.'));
		}
		if (this.isWriteLock) return Promise.resolve();
		return this.release().then( () => {
			return pasync.eachSeries(this.keys, (key) => {
				let client = this.locker.client.shard(key);
				return client.runScript('writeLock',
					this.locker.prefix + ':write:' + key,
					this.locker.prefix + ':read:' + key,
					token,
					options.lockTimeout || 60);
			});
		}).then( () => {
			this.isWriteLock = true;
			this.isLocked = true;
		});
	}

}

module.exports = RWLock;
