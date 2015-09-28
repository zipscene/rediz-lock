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
	 * @param {String[]} keys - The keys that are locked by this RWLock.
	 * @param {Boolean} [isWriteLock=false] - If true, this lock is a write lock.
	 */
	constructor(locker, keys, isWriteLock = false) {
		this.locker = locker;
		this.keys = keys;
		this.isWriteLock = isWriteLock;
		this.isLocked = true;
	}

	/**
	 * Releases all the locks held by this RWLock.
	 *
	 * @return {Promise} - Resolves when locks are released.
	 */
	release() {
		if (!this.isLocked) return Promise.resolve();
		if (!this.isWriteLock) {
			return pasync.eachSeries( this.keys.reverse(), (key) => {
				let client = this.locker.client.getShard(key);
				client.runScript('readLockRelease', this.locker.prefix + ':read:' + key);
			}).then( () => {
				this.isLocked = false;
			});
		} else {
			return pasync.eachSeries(this.keys.reverse(), (key) => {
				let client = this.locker.client.getShard(key);
				// TODO:: figure out where the token is
				return client.runScript('writeLockRelease', this.locker.prefix + ':write:' + key, token);
			}).then( () => {
				this.isLocked = false;
			});
		}

	}

	/**
	 * Upgrades a reader lock to a writer lock.
	 *
	 * @return {Promise}
	 */
	upgrade() {
		if (!this.isLocked) {
			return Promise.reject(new XError(XError.INTERNAL_ERROR, 'Cannot upgrade a lock that has been released.'));
		}
		if (this.isWriteLock) return Promise.resolve();
		return this.release().then( () => {
			return pasync.eachSeries(this.keys, (key) => {
				let client = this.locker.client.getShard(key);
				return client.runScript('writeLock', this.locker.prefix + ':write:' + key, this.locker.prefix + ':read:' + key, token, options.lockTimeout || 30);
			});
		}).then( () => {
			this.isWriteLock = true;
			this.isLocked = true;
		});
	}

}

module.exports = RWLock;
