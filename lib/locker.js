const path = require('path');
const pasync = require('pasync');
const uuid = require('node-uuid');
const Profiler = require('zs-simple-profiler');
const ResourceLockedError = require('./resource-locked-error');
const RWLock = require('./rwlock');
const LockSet = require('./lock-set');
const LockerBase = require('./locker-base');
const _ = require('lodash');

const profiler = new Profiler('Locker');

/**
 * Main class that acquires locks.
 *
 * @class Locker
 */
class Locker extends LockerBase {

	/**
	 * @constructor
	 * @param {RedizClient} redizClient - The RedizClient class to use to access redis.
	 * @param {Object} [options]
	 *   @param {String} [options.prefix='rzlock:'] - Optional redis key prefix
	 *   @param {Number} [options.lockTimeout=60] - Default lock timeout
	 *   @param {Number} [options.maxWaitTime=30] - Default max wait timeout
	 *   @param {Number} [options.downNodeExpiry=lockTimeout]
	 */
	constructor(redizClient, options = {}) {
		super();
		this.redizClient = redizClient;
		this.prefix = options.prefix || 'rzlock:';
		this.tokenBase = uuid.v4().substr(0, 17);
		this.tokenCtr = 1;
		this.scriptWaiter = pasync.waiter();
		// In the constructor, read in the lua script files and register them with redizClient
		this.redizClient.registerScriptDir(path.join(__dirname, '../../resources/lua'))
			.then(() => this.scriptWaiter.resolve(),
				(error) => this.scriptWaiter.reject(error)
			);
		this.defaults = {
			lockTimeout: (options.lockTimeout !== undefined) ? options.lockTimeout : 60,
			maxWaitTime: (options.maxWaitTime !== undefined) ? options.maxWaitTime : 30
		};
		this.defaults.downNodeExpiry = (options.downNodeExpiry !== undefined) ?
			options.downNodeExpiry : this.defaults.lockTimeout;
	}

	/**
	 * Creates a new lock set.
	 *
	 * @method createLockSet
	 * @return {LockSet}
	 */
	createLockSet() {
		return new LockSet(this);
	}

	/**
	 * Creates the next token for this locker
	 *
	 * @private
	 * @method _createToken
	 * @param {String} [tokenBase]
	 * @return {Number}
	 */
	_createToken(tokenBase) {
		return (tokenBase || this.tokenBase) + (this.tokenCtr++);
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
	 *   @param {Number} [options.downNodeExpiry] - Amount of time to wait if the redis
	 *     cluster node went down.
	 * @return {Promise{RWLock}} - Resolves with the RWLock instance which is used to release (or
	 *   upgrade) the lock instance.  Rejects with an XError.  If the lock cannot be acquired
	 *   because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError` (an XError
	 *   with a code of `XError.RESOURCE_LOCKED`).
	 */
	readLock(key, options = {}) {
		let prof = profiler.begin('#readLock');

		let { maxWaitTime, lockTimeout, downNodeExpiry } = _.defaults(options, this.defaults);

		return this.scriptWaiter.promise
			.then(() => {
				let client = this.redizClient.shard(key, { downNodeExpiry });
				return this._retryUntilTimeOut(
					() => {
						return client.runScript('readLock',
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							lockTimeout
						)
							.then((result) => {
								if (result === 1) return true;
								return false;
							});
					},
					maxWaitTime,
					key);
			})
			.then( () => {
				return new RWLock(this, key);
			})
			.then(prof.wrappedEnd());
	}

	/**
	 * Acquires a writer lock on a key.  As long as any thread has a writer lock on a key, no other
	 * threads can have either a reader or a writer lock.
	 *
	 * @method writeLock
	 * @param {String} key
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout]
	 *   @param {Number} [options.maxWaitTime]
	 *   @param {Boolean} [options.resolveConflicts=false] - If this is set to true, the following
	 *     behavior is enabled: If we attempt to acquire a write lock, but the write lock is
	 *     already held by another process, we either immediately fail with a RESOURCE_LOCKED error
	 *     or we wait for the lock, depending on a conflict resolution process.  The "winner" of
	 *     the conflict resolution (the process that continues to wait for the lock) is randomly
	 *     chosen (but the same "winner" will be chosen by both).  Note that the process that
	 *     already holds the lock will continue to hold it, even if it loses conflict resolution.
	 *     It is allowed to run to completion and release the lock.
	 *   @param {String} [options.tokenBase] - Internal option to override the generated base
	 *     token for write locking.  This is used by locksets for conflict resolution behavior
	 *     consistent on a per-lockset basis.
	 *   @param {Number} [options.downNodeExpiry] - Amount of time to wait if the redis
	 *     cluster node went down.
	 * @return {Promise{RWLock}}
	 */
	writeLock(key, options = {}) {
		let prof = profiler.begin('#writeLock');

		let { maxWaitTime, lockTimeout, downNodeExpiry } = _.defaults(options, this.defaults);

		let resolveConflicts = options.resolveConflicts || false;
		let token = this._createToken(options.tokenBase);
		let lostConflictResolution = false;
		return this.scriptWaiter.promise
			.then(() => {
				let writeLockClaimed = false;
				let client = this.redizClient.shard(key, { downNodeExpiry });
				return this._retryUntilTimeOut(
					() => {
						return client.runScript(
							(writeLockClaimed ? 'writeLockRetry' : 'writeLock'),
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							token,
							lockTimeout
						)
						.then((result) => {
							if (result[0] === 2) {
								writeLockClaimed = true;
								return false;
							} else if (result[0] === 1) {
								return true;
							} else {
								if (resolveConflicts) {
									if (result[1] < token) {
										lostConflictResolution = true;
										return true;
									}
								}
								writeLockClaimed = false;
								return false;
							}
						});
					},
					maxWaitTime,
					key);
			})
			.then(() => {
				if (lostConflictResolution) {
					throw new ResourceLockedError(key, 'Lost lock conflict resolution for: ' + key);
				} else {
					return new RWLock(this, key, token, true);
				}
			})
			.then(prof.wrappedEnd());
	}

	/**
	 * Continually tries to run the given function, until it timesout or successfully returns.
	 *
	 * @private
	 * @method _unlockRead
	 * @params {Function} func - a function that returns a promise, normally a script to run on the client.
	 *  The result should return true, or false.
	 *	if true, the promise will resolve, if fails, it will retry over, and over depending upon the timeout given.
	 * @params {Number} timeout - the amount of time in seconds to retry the given function.
	 * @params {String} key - the key that we are trying to run on the function. This is used for a reject error.
	 * @return {Promise}
	 */
	_retryUntilTimeOut(func, timeout, key) {
		let totalWaitTime = 0;
		let retry = (waitTime) => {
			let timeoutPromise = () => {
				if (!timeout) {
					return Promise.reject(new ResourceLockedError(key));
				}
				if (totalWaitTime / 1000 >= timeout) {
					return Promise.reject(
						new ResourceLockedError(key, 'Timed out trying to get a resource lock for: ' + key));
				}
				totalWaitTime += waitTime;
				return new Promise( (resolve) => {
					setTimeout( () => {
						let newWaitTime = waitTime * 3 + Math.floor(Math.random() * 3);
						if (newWaitTime > 2000) newWaitTime = 2000;
						return resolve(newWaitTime);
					}, waitTime);
				})
				.then( (time) => {
					return retry(time);
				});
			};
			return func().then( (result) => {
				if (!result) return timeoutPromise();
				return result;
			}).catch( (error) => {
				if (error && error.message !== 'Shard unavailable') throw error;
				return timeoutPromise();
			});
		};
		return retry(5);
	}

}

module.exports = Locker;
