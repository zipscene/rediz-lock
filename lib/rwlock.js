
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

	}

	/**
	 * Upgrades a reader lock to a writer lock.
	 *
	 * @return {Promise}
	 */
	upgrade() {
		if (!this.isLocked) return Promise.reject(new XError(XError.INTERNAL_ERROR, 'Cannot upgrade a lock that has been released.'));
		if (this.isWriteLock) return Promise.resolve();

	}

}

module.exports = RWLock;
