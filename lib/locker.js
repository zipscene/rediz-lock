const pasync = require('pasync');
const path = require('path');
const XError = require('xerror');
const RWLock = require('./rwlock');
const LockSet = require('./lock-set');
const _ = require('lodash');
const uuid = require('node-uuid');
const TOKEN_BASE = uuid.v4().substr(0, 17);
/**
 * Main class that acquires locks.
 *
 * @class Locker
 */
class Locker {

	/**
	 * @constructor
	 * @param {RedizClient} redizClient - The RedizClient class to use to access redis.
	 * @param {Object} [options]
	 *   @param {String} [options.prefix='rzlock:'] - Optional redis key prefix
	 */
	constructor(redizClient, options = {}) {
		this.redizClient = redizClient;
		this.prefix = options.prefix || 'rzlock:';
		this.tokenCtr = 1;
		this.scriptWaiter = pasync.waiter();
		// In the constructor, read in the lua script files and register them with redizClient
		this.redizClient.registerScriptDir(path.join(__dirname, '../../resources/lua'))
			.then(() => this.scriptWaiter.resolve(),
				(error) => this.scriptWaiter.reject(error)
			);
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
	 * Alias of `writeLock()`
	 *
	 * @method lock
	 */
	lock(key, options) {
		return this.writeLock(key, options);
	}

	/**
	 * Creates the next token for this locker
	 *
	 * @method createToken
	 * @return {Number}
	 */
	createToken() {
		return TOKEN_BASE + (this.tokenCtr++);
	}

	/**
	 * Acquires a reader lock on a key.  Multiple threads may have reader locks on the same key,
	 * but no thread may have a reader lock if any thread has a writer lock.
	 *
	 * @method readLock
	 * @param {String|String[]} keys - The string key to lock on, or an array of keys to
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
	readLock(keys, options = {}) {
		let lockedKeys = [];
		if (!_.isArray(keys)) { keys = [ keys ]; }
		let maxWaitTime = options.maxWaitTime || options.maxWaitTime === 0 ? options.maxWaitTime : 30;
		let lockTimeout = options.lockTimeout || 60;
		return this.scriptWaiter.promise.then(() => {
			return pasync.eachSeries(keys, (key) => {
				let client = this.redizClient.shard(key, { downNodeExpiry: lockTimeout });
				return this._retryUntilTimeOut(
						() => {
							return client.runScript('readLock',
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							lockTimeout
							).then( (result) => {
								if (result === 1) {
									lockedKeys.push(key);
									return true;
								} else {
									return false;
								}
							});
						},
						maxWaitTime,
						key,
						'read'
					);
			});
		}).then( () => {
			return new RWLock(this, lockedKeys);
		}).catch( (error) => {
			if (lockedKeys.length) {
				this._unlockRead(lockedKeys).then( () => {
					throw error;
				});
			} else {
				throw error;
			}
		});
	}

	_unlockRead(keys) {
		return pasync.eachSeries(keys.reverse(), (key) => {
			let client = this.redizClient.shard(key, { downNodeExpiry: 0 });
			return client.runScript('readLockRelease', this.prefix + ':read:' + key);
		});
	}

	_retryUntilTimeOut(promise, timeout, key, type) {
		let totalWaitTime = 0;
		let retry = (waitTime) => {
			let timeoutPromise = () => {
				return new Promise( (resolve, reject) => {
					if (!timeout) {
						throw new XError(XError.RESOURCE_LOCKED, 'Resource Locked',
							{ key: key, timeout: timeout, type: type });
					}
					if (totalWaitTime / 1000 >= timeout) {
						throw new XError(XError.RESOURCE_LOCKED, 'Time out waiting for lock: ' + key,
							{ key: key, timeout: timeout, type: type });
					}
					totalWaitTime += waitTime;
					setTimeout( () => {
						let newWaitTime = waitTime * 3 + Math.floor(Math.random() * 3);
						if (newWaitTime > 2000) newWaitTime = 2000;
						return resolve(newWaitTime);
					}, waitTime);
				}).then( (time) => {
					return retry(time);
				});
			};
			return promise().then( (result) => {
				if (!result) return timeoutPromise();
				return result;
			}).catch( (error) => {
				if (error && error.message !== 'Shard unavailable') throw error;
				return timeoutPromise();
			});
		};
		return retry(5);
	}

	/**
	 * Acquires a writer lock on a key.  As long as any thread has a writer lock on a key, no other
	 * threads can have either a reader or a writer lock.
	 *
	 * @method writeLock
	 * @param {String|String[]} keys
	 * @param {Object} [options={}]
	 *   @param {Number} [options.lockTimeout=60]
	 *   @param {Number} [options.maxWaitTime=30]
	 * @return {Promise{RWLock}}
	 */
	writeLock(keys, options = {}) {
		let lockedKeys = [];
		let lockedTokens = [];
		if (!_.isArray(keys)) { keys = [ keys ]; }
		let maxWaitTime = options.maxWaitTime || options.maxWaitTime === 0 ? options.maxWaitTime : 30;
		let lockTimeout = options.lockTimeout || 60;
		return this.scriptWaiter.promise.then(() => {
			return pasync.eachSeries(keys, (key) => {
				let writeLockClaimed = false;
				let token = this.createToken();
				let client = this.redizClient.shard(key, { downNodeExpiry: lockTimeout });
				return this._retryUntilTimeOut(
					() => {
						let funcName = writeLockClaimed ? 'writeLockRetry' : 'writeLock';
						return client.runScript(funcName,
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							token,
							lockTimeout
						).then( (result) => {
							console.log(result);
							if (result === 2) {
								console.log('write lock claimed');
								writeLockClaimed = true;
								return false;
							} else if (result === 1) {
								lockedKeys.push(key);
								lockedTokens.push(token);
								return true;
							} else {
								writeLockClaimed = false;
								return false;
							}
						});
					},
					maxWaitTime,
					key,
					'write'
				);
			});
		}).then( () => {
			return new RWLock(this, lockedKeys, lockedTokens, true);
		}).catch( (error) => {
			if (lockedKeys.length) {
				this._unlockWrite(lockedKeys, lockedTokens).then( () => {
					throw error;
				});
			} else {
				throw error;
			}
		});
	}

	_unlockWrite(keys, lockedTokens) {
		let tokens = lockedTokens.reverse();
		let count = -1;
		return pasync.eachSeries(keys.reverse(), (key) => {
			count = count + 1;
			let client = this.redizClient.shard(key, { downNodeExpiry: 0 });
			return client.runScript('writeLockRelease', this.prefix + ':write:' + key, tokens[count]);
		});
	}

	/**
	 * Wraps a function in a read lock.  The read lock is acquired before the function is executed,
	 * and is released when the function returns, regardless of whether or not it errors.  The
	 * function can be synchronous, or it can return a Promise.  If it returns a Promise, the
	 * resolve/reject values are forwarded to the result of `readLockWrap()` .
	 *
	 * @method readLockWrap
	 * @param {String|String[]} keys
	 * @param {Object} [options={}]
	 * @param {Function} fn - Function that is executed when the lock is acquired.
	 * @return {Promise} - Resolves or rejects with the return value of `fn` .
	 */
	readLockWrap(keys, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		if (!_.isArray(keys)) { keys = [ keys ]; }
		return this.scriptWaiter.promise.then(() => {
			this.readLock(keys);
		}).then( (rwlock) => {
			let result = rwlock.locker.fn();
			rwlock.release().then( () => {
				return result;
			});
		});
	}

	/**
	 * Wraps a function in a write lock.
	 *
	 * @method writeLockWrap
	 * @param {String|String[]} key
	 * @param {Object} [options={}]
	 * @param {Function} fn
	 * @return {Promise}
	 */
	writeLockWrap(key, options, fn) {
		if (_.isFunction(options)) { fn = options; options = {}; }
		if (!_.isArray(key)) { key = [ key ]; }
		this.scriptWaiter.promise.then(() => {
			// Do actual stuff here
		});
	}

}

module.exports = Locker;
