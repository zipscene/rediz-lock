// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const path = require('path');
const pasync = require('pasync');
const Profiler = require('simprof');
const ResourceLockedError = require('./resource-locked-error');
const RWLock = require('./rwlock');
const LockSet = require('./lock-set');
const LockerBase = require('./locker-base');
const DistributedWriteLock = require('./distributed-write-lock');
const XError = require('xerror');
const _ = require('lodash');
const objtools = require('objtools');
const os = require('os');
const process = require('process');

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
	 *   @param {Number} [options.lockTimeout=10] - Default lock timeout
	 *   @param {Number} [options.maxWaitTime=86400] - Default max wait timeout
	 *   @param {Number} [options.downNodeExpiry=lockTimeout]
	 *   @param {Boolean} [options.debugTokens=false] - If set to true, debug information
	 *     is included with lock tokens.  This can help diagnose issues but comes at a
	 *     performance cost.
	 *   @param {Number} [options.minDistributedLockFlagExpireTime=5] - Minimum time in the
	 *     future the distributed lock flag can expire.
	 *   @param {Number} [options.maxDistributedLockFlagExpireTime=60] - Max amount of time
	 *     in the future that the distributed lock flag expires.
	 *   @param {Number} [options.distributedLockFlagTimerWindow=15] - Amount of time before
	 *     the min flag expiry that processes should start checking for the lock flag.
	 */
	constructor(redizClient, options = {}) {
		super();
		this.redizClient = redizClient;
		this.prefix = options.prefix || 'rzlock:';
		this.tokenBase = this._generateTokenBase();
		this.tokenCtr = 1;
		this.scriptWaiter = pasync.waiter();
		// In the constructor, read in the lua script files and register them with redizClient
		this.redizClient.registerScriptDir(path.join(__dirname, '../resources/lua'))
			.then(() => this.scriptWaiter.resolve(),
				(error) => this.scriptWaiter.reject(error)
			);
		let defaultOpts = {
			lockTimeout: 60,
			maxWaitTime: 86400,
			warnTime: undefined,
			minDistributedLockFlagExpireTime: 5,
			maxDistributedLockFlagExpireTime: 60,
			distributedLockFlagTimerWindow: 15
		};
		this.defaults = _.defaults({}, options, defaultOpts);
		this.debugTokens = options.debugTokens || false;
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
	 * @param {Number} [priority=50] - Priority of token for conflict resolution locks. Lower priorities are better
	 *   (Less likely to lose conflict resolution). Numbers are from 0 to 99.
	 * @param {Object} [options] - Lock options
	 * @return {Number}
	 */
	_createToken(tokenBase, priority = 50, options = {}) {
		let priStr;
		if (priority < 10) {
			priStr = '0' + priority;
		} else {
			priStr = '' + priority;
		}
		let token = priStr + (tokenBase || this.tokenBase) + (this.tokenCtr++);
		if (this.debugTokens) {
			let debugObj = {
				name: options.name,
				info: options.info,
				hostname: os.hostname(),
				pid: process.pid,
				caller: options.caller
			};
			token += ' !!DEBUG!! ' + JSON.stringify(debugObj);
		}
		return token;
	}

	/**
	 * Parses the embedded information in a lock debug token.
	 *
	 * @method _parseDebugToken
	 * @private
	 * @param {String|Array} token
	 * @return {Object}
	 */
	_parseDebugToken(token) {
		if (!token) return undefined;
		if (Array.isArray(token)) {
			return token.map((t) => this._parseDebugToken(t));
		}
		if (this.debugTokens) {
			let parts = token.split(' !!DEBUG!! ');
			if (parts.length >= 2) {
				let json = parts.slice(1).join(' !!DEBUG!! ');
				let obj = JSON.parse(json);
				obj.token = parts[0];
				return obj;
			}
		}
		return { token };
	}

	_captureStack(options = {}, extraDepth = 0) {
		if (!options.caller && this.debugTokens) {
			options.caller = new Error().stack.split('\n')[3 + extraDepth];
		}
	}

	/**
	 * Distributed lock flags are redis keys that are set when doing a distributed read lock.
	 * They indicate that a distributed read lock is active (or recently active) for a lock
	 * key and indicate to the write lock that a distributed write lock must be obtained.  To
	 * enable this behavior, pass the `enableDistributedAuto` option to `readLock()` and set
	 * the `distributed` option to "auto" in `writeLock()`.
	 *
	 * @method _checkAndSetDistributedLockFlag
	 * @private
	 * @param {String} key
	 * @return {Number} - The number of seconds the re-check timer should be set for.
	 */
	async _checkAndSetDistributedLockFlag(key, options = {}) {
		await this.scriptWaiter.promise;

		let {
			downNodeExpiry,
			minDistributedLockFlagExpireTime,
			maxDistributedLockFlagExpireTime,
			distributedLockFlagTimerWindow
		} = _.defaults(options, this.defaults);
		let redisKey = this.prefix + ':dflag:' + key;
		let distributedShard = Math.floor(Math.random() * this.redizClient.getNumShards());
		let client = this.redizClient.shard(distributedShard, { downNodeExpiry });

		let result = await client.ttl(redisKey);
		let expTime;
		if (result === -1) {
			// Weird, the key doesn't expire.  Shouldn't happen, but not necessarily going to break anything.
			// Return a arbitrary value for the timer recheck period.
			return 600;
		} else if (result === -2 || (result >= 0 && result < minDistributedLockFlagExpireTime + 1)) {
			// Flag doesn't yet exist, or expires in near future.  Set it on all shards.
			for (let shardNum = 0; shardNum < this.redizClient.getNumShards(); shardNum++) {
				await this.redizClient.shard(shardNum).set(redisKey, '1', 'EX', maxDistributedLockFlagExpireTime);
			}
			expTime = maxDistributedLockFlagExpireTime;
		} else if (result >= 0) {
			expTime = result;
		} else {
			throw new Error('Unexpected reply from Redis');
		}

		// Wait a randomized amount of time before checking again to prevent several nodes
		// checking at the same time.
		let windowSpan = distributedLockFlagTimerWindow;
		if (windowSpan < expTime - minDistributedLockFlagExpireTime) {
			windowSpan = expTime - minDistributedLockFlagExpireTime;
		}
		// TODO: Select a random value on an exponential curve rather than linear
		let delay = expTime - Math.floor(Math.random() * windowSpan + minDistributedLockFlagExpireTime);
		return delay;
	}

	async _checkDistributedLockFlag(key, options = {}) {
		let { downNodeExpiry } = _.defaults(options, this.defaults);
		let redisKey = this.prefix + ':dflag:' + key;
		let distributedShard = Math.floor(Math.random() * this.redizClient.getNumShards());
		let client = this.redizClient.shard(distributedShard, { downNodeExpiry });
		let result = await client.exists(redisKey);
		return (result === 0) ? false : true;
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
	 *   @param {Number} [options.heartbeatInterval] - Amount of time (in milliseconds) between
	 *     heartbeats to update the lock expiry.  Defaults to a third of lockTimeout.  Set to
	 *     false to disable heartbeats.
	 *   @param {Number} [options.heartbeatTimeout] - What each heartbeat should update the timeout
	 *     to.  Defaults to three times the heartbeatInterval.
	 *   @param {Number} [options.warnTime] - If supplied, after `warnTime` number of seconds of
	 *     waiting for the lock, a warning is printed to stderr.
	 *   @param {Boolean} [options.distributed=false] - If set to true, put the read lock in distributed
	 *     mode, for high-traffic locks.  In effect, this assigns the read lock to a random shard.
	 *   @param {Boolean} [options.enableDistributedAuto=true] - If true, and `distributed` is also
	 *     true, this enabled distributed lock detection for the corresponding write locks when
	 *     "auto" is passed as the `distributed` option to `writeLock()`.
	 * @return {Promise{RWLock}} - Resolves with the RWLock instance which is used to release (or
	 *   upgrade) the lock instance.  Rejects with an XError.  If the lock cannot be acquired
	 *   because of a `maxWaitTime` timeout, this rejects with a `ResourceLockedError` (an XError
	 *   with a code of `XError.RESOURCE_LOCKED`).
	 */
	async readLock(key, options = {}) {
		this._captureStack(options);

		return await profiler.run('#readLock', async() => {

			let lastLockHolder = null;
			let numLockHolders = 0;
			let outputWarningMessage = false;
			let distributedShard = null;

			let { maxWaitTime, lockTimeout, downNodeExpiry, heartbeatInterval, heartbeatTimeout, warnTime } =
				_.defaults(options, this.defaults);
			if (heartbeatInterval === undefined) {
				heartbeatInterval = lockTimeout ? (Math.floor(lockTimeout * 1000 / 3)) : false;
			}
			if (heartbeatTimeout === undefined && heartbeatInterval) {
				heartbeatTimeout = Math.ceil(heartbeatInterval * 3 / 1000);
			}

			let token = this._createToken(options.tokenBase, 0, options);


			try {
				let token = this._createToken(options.tokenBase, 0, options);

				await this.scriptWaiter.promise;

				distributedShard = key;
				if (options.distributed) {
					distributedShard = Math.floor(Math.random() * this.redizClient.getNumShards());
				}
				let client = this.redizClient.shard(distributedShard, { downNodeExpiry });

				await this._retryUntilTimeOut(
					async() => {
						if (options.distributed && options.enableDistributedAuto !== false) {
							await this._checkAndSetDistributedLockFlag(key, options);
						}

						let result = await client.runScript('readLock',
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							token,
							lockTimeout
						);
						if (result[0] === 1) {
							// Acquired a shared read lock
							return true;
						}
						// A write lock already exists on this key
						if (lastLockHolder !== null && result[1] !== lastLockHolder) {
							lastLockHolder = result[1];
							numLockHolders++;
							return 'reset';
						}
						lastLockHolder = result[1];
						return false;
					},
					maxWaitTime,
					key,
					warnTime,
					(key, time) => {
						outputWarningMessage = true;
						console.warn(`Taking a long time to acquire read lock ${key}`, {
							key,
							lockType: 'read',
							maxWaitTime,
							ownTokenBase: this.tokenBase,
							ownToken: token,
							holder: lastLockHolder,
							numHolders: numLockHolders,
							currentWaitTime: time,
							ownDebug: this._parseDebugToken(token),
							holderDebug: this._parseDebugToken(lastLockHolder)
						});
					}
				);

				let distributedFlagCheckTime = null;
				if (options.distributed && options.enableDistributedAuto) {
					distributedFlagCheckTime = await this._checkAndSetDistributedLockFlag(key, options);
				}

				if (outputWarningMessage) {
					console.log(`Read lock on ${key} eventually obtained.`);
				}

				let rwlock = new RWLock(this, key, token, distributedShard, false, heartbeatInterval, heartbeatTimeout);
				if (distributedFlagCheckTime !== null) {
					rwlock._startDistributedLockFlagCheckTimer(distributedFlagCheckTime, options);
				}
				return rwlock;

			} catch (err) {
				if (err.code === XError.RESOURCE_LOCKED) {
					if (!err.data) err.data = {};
					err.data.key = key;
					err.data.lockType = 'read';
					err.data.maxWaitTime = maxWaitTime;
					err.data.ownToken = token;
					err.data.ownTokenBase = options.tokenBase || this.tokenBase;
					err.data.holder = lastLockHolder;
					err.data.numHolders = numLockHolders;
					err.data.ownDebug = this._parseDebugToken(token);
					err.data.holderDebug = this._parseDebugToken(lastLockHolder);
				}
				throw err;
			}
		});
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
	 *   @param {Number} [options.conflictPriority=50] - Conflict resolution priority.  Number
	 *     from 0 to 99. Lower is better (less likely to lose conflict resolution).
	 *   @param {String} [options.tokenBase] - Internal option to override the generated base
	 *     token for write locking.  This is used by locksets for conflict resolution behavior
	 *     consistent on a per-lockset basis.
	 *   @param {Number} [options.downNodeExpiry] - Amount of time to wait if the redis
	 *     cluster node went down.
	 *   @param {Number} [options.heartbeatInterval]
	 *   @param {Number} [options.heartbeatTimeout]
	 *   @param {Number} [options.warnTime]
	 *   @param {Boolean|"auto"} [options.distributed=false] - If true, enables a distributed write
	 *     lock.  If "auto" checks for the distributed lock flag to determine whether to do a
	 *     distributed lock or not.
	 * @return {Promise{RWLock}}
	 */
	async writeLock(key, options = {}) {
		if (options.distributed) {
			// Perform a distributed write lock
			return await this._writeLockDistributed(key, options);
		}

		this._captureStack(options);

		return await profiler.run('#writeLock', async() => {

			let lastLockHolder = null;
			let numLockHolders = 0;
			let outputWarningMessage = false;

			let { maxWaitTime, lockTimeout, downNodeExpiry, heartbeatInterval, heartbeatTimeout, warnTime } =
				_.defaults(options, this.defaults);
			if (heartbeatInterval === undefined) {
				heartbeatInterval = lockTimeout ? (Math.floor(lockTimeout * 1000 / 3)) : false;
			}
			if (heartbeatTimeout === undefined && heartbeatInterval) {
				heartbeatTimeout = Math.ceil(heartbeatInterval * 3 / 1000);
			}

			let resolveConflicts = options.resolveConflicts || false;
			let conflictPriority = (options.conflictPriority === undefined) ? 50 : options.conflictPriority;
			let token = this._createToken(options.tokenBase, conflictPriority, options);
			let lostConflictResolution = false;

			await this.scriptWaiter.promise;

			let writeLockClaimed = false;
			let shardKey = (options._forceShardKey === undefined) ? key : options._forceShardKey;
			let client = this.redizClient.shard(shardKey, { downNodeExpiry });
			// Keep trying to establish the lock until we get it or exceed the maximum wait time
			try {
				await this._retryUntilTimeOut(
					async() => {
						let result = await client.runScript(
							(writeLockClaimed ? 'writeLockRetry' : 'writeLock'),
							this.prefix + ':write:' + key,
							this.prefix + ':read:' + key,
							token,
							lockTimeout
						);
						if (result[0] === 2) {
							// We have successfully claimed the write lock, but a read lock on the
							// same key already exists, so we have to wait until all read locks are
							// released to complete exclusive acquisition of the write lock.
							let retVal = writeLockClaimed ? false : 'reset';
							writeLockClaimed = true;
							lastLockHolder = result[1];
							return retVal;
						} else if (result[0] === 1) {
							// Successfully acquired the write lock and there's no existing read lock.
							// Lock acquisition is complete.
							return true;
						} else {
							// There is an existing write lock held by token result[1]
							if (resolveConflicts) {
								// If conflict resolution mode, the lock with the lower token "wins"
								if (result[1] < token) {
									lostConflictResolution = true;
									lastLockHolder = result[1];
									numLockHolders++;
									return true;
								}
							}
							writeLockClaimed = false;
							if (
								lastLockHolder !== null &&
								lastLockHolder !== undefined &&
								lastLockHolder.length !== 0 &&
								!objtools.deepEquals(lastLockHolder, result[1])
							) {
								// Every time the lock changes hands, reset the wait timer.
								lastLockHolder = result[1];
								numLockHolders++;
								return 'reset';
							} else if (!lastLockHolder) {
								lastLockHolder = result[1];
							}
							return false;
						}
					},
					maxWaitTime,
					key,
					warnTime,
					(key, time) => {
						outputWarningMessage = true;
						console.warn(`Taking a long time to acquire write lock ${key}`, {
							key,
							lockType: 'write',
							maxWaitTime,
							ownToken: token,
							ownTokenBase: options.tokenBase || this.tokenBase,
							holder: lastLockHolder,
							numHolders: numLockHolders,
							currentWaitTime: time,
							ownDebug: this._parseDebugToken(token),
							holderDebug: this._parseDebugToken(lastLockHolder)
						});
					}
				);
			} catch (err) {
				// Make sure any claimed locks are cleaned up on error
				if (writeLockClaimed) {
					client.runScript('writeLockRelease', this.prefix + ':write:' + key, token);
				}
				if (err.code === XError.RESOURCE_LOCKED) {
					if (!err.data) err.data = {};
					err.data.key = key;
					err.data.lockType = 'write';
					err.data.maxWaitTime = maxWaitTime;
					err.data.ownToken = token;
					err.data.ownTokenBase = this.tokenBase;
					err.data.holder = lastLockHolder;
					err.data.numHolders = numLockHolders;
					err.data.ownDebug = this._parseDebugToken(token);
					err.data.holderDebug = this._parseDebugToken(lastLockHolder);
				}
				throw err;
			}


			if (lostConflictResolution) {
				throw new ResourceLockedError(key, 'Lost lock conflict resolution for: ' + key, {
					key,
					lockType: 'write',
					maxWaitTime,
					ownToken: token,
					ownTokenBase: this.tokenBase,
					holder: lastLockHolder,
					numHolders: numLockHolders
				});
			} else {
				if (outputWarningMessage) {
					console.warn(`Write lock on ${key} eventually obtained.`);
				}
				return new RWLock(
					this,
					key,
					token,
					(options._forceShardKey === undefined) ? null : options._forceShardKey,
					true,
					heartbeatInterval,
					heartbeatTimeout
				);
			}
		});
	}

	/**
	 * Like writeLock(), but performs a distributed write lock by locking the key on all shards.
	 *
	 * @method _writeLockDistributed
	 * @return {Promise}
	 */
	async _writeLockDistributed(key, options) {
		await this.scriptWaiter.promise;

		if (options.distributed === 'auto') {
			// Maybe do a distributed lock, maybe not.  Need to check the flag.
			let needDistributed = await this._checkDistributedLockFlag(key, options);
			if (!needDistributed) {
				// Can just do a normal lock, but check the flag again afterwards.
				let opts = objtools.deepCopy(options);
				delete opts.distributed;
				let lock = await this.writeLock(key, opts);
				let flagSetNow = await this._checkDistributedLockFlag(key, options);
				if (flagSetNow) {
					// A distributed read lock has started since starting this write lock
					// Release the write lock and get a distributed one
					await lock.forceRelease();
				} else {
					return lock;
				}
			}
		}

		let numShards = this.redizClient.getNumShards();
		let locks = [];
		try {
			for (let shard = 0; shard < numShards; shard++) {
				let opts = objtools.deepCopy(options);
				opts._forceShardKey = shard;
				delete opts.distributed;
				let lock = await this.writeLock(key, opts);
				locks.push(lock);
			}
		} catch (ex) {
			for (let lock of locks) {
				try {
					await lock.forceRelease();
				} catch (ex2) {
					console.warn('Got additional error unlocking remaining locks');
					console.warn(ex2);
				}
			}
			throw ex;
		}
		return new DistributedWriteLock(locks);
	}

	/**
	 * Continually tries to run the given function, until it timesout or successfully returns.
	 *
	 * @private
	 * @method _retryUntilTimeOut
	 * @param {Function} func - a function that returns a promise, normally a script to run on the client.
	 *  The result should return true, or false.
	 *	if true, the promise will resolve, if fails, it will retry over, and over depending upon the timeout given.
	 *  The special value "reset" can also be returned which resets the retry time.
	 * @param {Number} timeout - the amount of time in seconds to retry the given function.
	 * @param {String} key - the key that we are trying to run on the function. This is used for a reject error.
	 * @param {Number} warnTime - If this is given, the supplied `warn` function will be called after this
	 *   number of seconds.  It will only be called once.
	 * @param {Function} warn - A function to display a warning that we're waiting a long time on a lock.  This
	 *   function has the signature function(key, totalWaitTime)
	 * @return {Promise}
	 */
	_retryUntilTimeOut(func, timeout, key, warnTime, warn) {
		let totalWaitTime = 0;
		let initialWaitTime = 5;
		let calledWarn = false;
		let retry = (waitTime) => {
			let timeoutPromise = (resetWaitTime) => {
				if (!timeout) {
					return Promise.reject(new ResourceLockedError(key));
				}
				if (totalWaitTime / 1000 >= timeout) {
					return Promise.reject(
						new ResourceLockedError(key, 'Timed out trying to get a resource lock for: ' + key));
				}
				totalWaitTime += waitTime;
				if (warnTime && totalWaitTime >= warnTime * 1000 && !calledWarn) {
					calledWarn = true;
					if (warn) {
						warn(key, Math.ceil(totalWaitTime / 1000));
					}
				}
				return new Promise( (resolve) => {
					setTimeout( () => {
						let newWaitTime = waitTime * 3 + Math.floor(Math.random() * 3);
						if (newWaitTime > 1000) newWaitTime = 1000;
						if (resetWaitTime) newWaitTime = resetWaitTime;
						return resolve(newWaitTime);
					}, waitTime);
				})
					.then( (time) => {
						return retry(time);
					});
			};
			return func().then( (result) => {
				if (result === 'reset') {
					return timeoutPromise(initialWaitTime);
				} else if (!result) {
					return timeoutPromise();
				} else {
					return result;
				}
			}).catch( (error) => {
				if (error && error.message !== 'Shard unavailable') throw error;
				return timeoutPromise();
			});
		};
		return retry(initialWaitTime);
	}

}

module.exports = Locker;
