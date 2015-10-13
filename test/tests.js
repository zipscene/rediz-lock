const expect = require('chai').expect;
const sinon = require('sinon');
const Locker = require('../lib/locker');
const RedizClient = require('zs-rediz');
const LockSet = require('../lib/lock-set');
const RWLock = require('../lib/rwlock');
const XError = require('xerror');
const REDIZ_CONFIG = {
	host: 'localhost',
	port: '6379',
	volatileCluster: true
};

describe('Class Locker', () => {
	let redizClient, locker;
	beforeEach( (done) => {
		redizClient = new RedizClient(REDIZ_CONFIG);
		locker = new Locker(redizClient);
		done();
	});

	it('construct a locker with a redis client, and a scriptWaiter', () => {
		let redizClient = new RedizClient(REDIZ_CONFIG);
		let locker = new Locker(redizClient);
		expect(locker.redizClient).to.be.an.instanceof(RedizClient);
		expect(locker.prefix).to.equal('rzlock:');
		expect(locker.scriptWaiter).to.be.a('object');
		return locker.scriptWaiter.promise.then( () => {
			expect(locker.redizClient.registeredScripts).to.be.not.empty;
			expect(locker.redizClient.registeredScripts.readLock).to.exist;
			expect(locker.redizClient.registeredScripts.readLockRelease).to.exist;
			expect(locker.redizClient.registeredScripts.writeLock).to.exist;
			expect(locker.redizClient.registeredScripts.writeLockRelease).to.exist;
			expect(locker.redizClient.registeredScripts.writeLockRetry).to.exist;
		});
	});

	describe('#createLockSet', () => {
		it('should create a new lock set instance', (done) => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			let lockSet = locker.createLockSet();
			expect(lockSet).to.be.an.instanceof(LockSet);
			expect(lockSet.locks).to.exist;
			expect(lockSet.locks).to.be.empty;
			expect(lockSet.locks).to.be.an('object');
			expect(lockSet.dependentLockSets).to.exist;
			expect(lockSet.dependentLockSets).to.be.empty;
			expect(lockSet.dependentLockSets).to.be.instanceof(Array);
			done();
		});
	});

	describe('#readLock', () => {
		let writeLockMock;
		after( (done) => {
			if (writeLockMock) {
				writeLockMock.restore();
				done();
			} else {
				done();
			}
		});

		it('should lock read for a single key and release it', () => {
			let readLock;
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.key).to.equal('key');
				expect(rwlock.locker).to.equal(locker);
				return rwlock.release();
			}).then( () => {
				expect(readLock.isLocked).to.equal(false);
			}).catch( (error) => {
				if (readLock) {
					return readLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should time out when trying to access an already read locked key', () => {
			let readLock, writeLock;
			return locker.writeLock('key', { maxWaitTime: 0, lockTimeout: 10 }).then( (rwlock) => {
				writeLock = rwlock;
				return locker.readLock('key', { maxWaitTime: 0 });
			}).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLock) {
					return writeLock.release().then( () => {
						if (readLock) {
							return readLock.release().then( () => {
								throw error;
							});
						} else {
							expect(error).to.exist;
							expect(error).to.be.an.instanceof(XError);
							expect(error.code).to.equal(XError.RESOURCE_LOCKED);
							expect(error.message).to.equal('A lock cannot be acquired on the resource: key');
						}
					});
				} else {
					throw error;
				}
			});
		});

		it('should time out when maxWaitTime is reached', function() {
			this.timeout(5000);
			let readLock, writeLock;
			return locker.writeLock('key', { maxWaitTime: 0, lockTimeout: 10 }).then( (rwlock) => {
				writeLock = rwlock;
				return locker.writeLock('key', { maxWaitTime: 2 });
			}).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLock) {
					return writeLock.release().then( () => {
						if (readLock) {
							return readLock.release().then( () => {
								throw error;
							});
						} else {
							expect(error).to.exist;
							expect(error).to.be.an.instanceof(XError);
							expect(error.code).to.equal(XError.RESOURCE_LOCKED);
							expect(error.message).to.equal('Timed out trying to get a resource lock for: key');
						}
					});
				} else {
					throw error;
				}
			});
		});

		it('should lock a read, and then upgrade them to writes', () => {
			let readLock;
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.exist;
				expect(rwlock.key).to.equal('key');
				expect(rwlock.isWriteLock).to.equal(false);
				return rwlock.upgrade();
			}).then( () => {
				expect(readLock.isWriteLock).to.equal(true);
				expect(readLock.token).to.exist;
				expect(readLock.key).to.equal('key');
				return readLock.release();
			}).catch( (error) => {
				if (readLock) {
					return readLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should attempts to upgrade, fails and releases all the locks', () => {
			let readLock;
			writeLockMock = sinon.mock(locker);
			writeLockMock.expects('writeLock').once().throws(new XError(XError.INTERNAL_ERROR));
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.exist;
				expect(rwlock.key).to.equal('key');
				expect(rwlock.isWriteLock).to.equal(false);
				return rwlock.upgrade({ onError: 'release' });
			}).then( () => {
				throw new Error();
			}).catch( (error) => {
				if (readLock.isLocked) {
					return readLock.release().then( () => {
						throw Error('This test should have unlocked the readlock');
					});
				} else {
					writeLockMock.verify();
					expect(readLock.isLocked).to.equal(false);
					expect(readLock.key).to.equal('key');
				}
			});
		});
	});

	describe('#readLockSet', function() {
		it('should reject with an error if keys is not an array', function() {
			return locker.readLockSet('key')
			.catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				expect(error.code).to.equal(XError.INVALID_ARGUMENT);
				expect(error.message).to.equal('keys must be an array');
			});
		});

		it('should return a new lock set with the read locks', function() {
			let returnedLockSet;
			return locker.readLockSet([ 'key', 'key1' ], { maxWaitTime: 0 })
			.then( (newLockSet) => {
				returnedLockSet = newLockSet;
				expect(newLockSet.locks.key).to.exist;
				expect(newLockSet.locks.key.key).to.equal('key');
				expect(newLockSet.locks.key.isWriteLock).to.be.false;
				expect(newLockSet.locks.key1).to.exist;
				expect(newLockSet.locks.key1.key).to.equal('key1');
				expect(newLockSet.locks.key1.isWriteLock).to.be.false;
				return newLockSet;
			})
			.then( (newLockSet) => {
				return newLockSet.release();
			})
			.catch( (error) => {
				if (returnedLockSet) {
					return returnedLockSet.release().then( () => { throw error; });
				} else {
					throw error;
				}
			});
		});

		it('should return a the given lock set with new read locks appended', function() {
			let returnedLockSet;
			let lockSet = locker.createLockSet();
			return locker.readLock('key')
			.then( (readLock) => lockSet.addLock(readLock))
			.then( () => {
				expect(lockSet.locks.key).to.exist;
				expect(lockSet.locks.key.isWriteLock).to.be.false;
			})
			.then( () => {
				return locker.readLockSet([ 'key1', 'key2' ], { maxWaitTime: 0, lockSet });
			})
			.then( (lockSet) => {
				expect(lockSet.locks.key).to.exist;
				expect(lockSet.locks.key.key).to.equal('key');
				expect(lockSet.locks.key.isWriteLock).to.be.false;
				expect(lockSet.locks.key1).to.exist;
				expect(lockSet.locks.key1.key).to.equal('key1');
				expect(lockSet.locks.key1.isWriteLock).to.be.false;
				expect(lockSet.locks.key2).to.exist;
				expect(lockSet.locks.key2.key).to.equal('key2');
				expect(lockSet.locks.key2.isWriteLock).to.be.false;
				return lockSet.release();
			})
			.catch( (error) => {
				return lockSet.release()
				.then( () => { throw error; });
			});
		});
	});

	describe('#writeLock', () => {

		it('should lock write for a single key', () => {
			let writeLocker;
			return locker.writeLock('key')
			.then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.token).to.exist;
				expect(rwlock.key).to.equal('key');
				expect(rwlock.locker).to.equal(locker);
				expect(rwlock.isWriteLock).to.equal(true);
				return rwlock.release();
			})
			.then( () => {
				expect(writeLocker.isLocked).to.equal(false);
			})
			.catch( (error) => {
				if (writeLocker) {
					return writeLocker.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should fail when trying to lock a previously locked write locker', () => {
			let writeLocker, writeLocker2;
			return locker.writeLock('key', { maxWaitTime: 0 })
			.then( (rwlock) => {
				writeLocker = rwlock;
				return locker.writeLock('key', { maxWaitTime: 0 });
			})
			.then( (rwlock) => {
				writeLocker2 = rwlock;
				throw new Error('Should not have locked the second lock');
			})
			.catch( (error) => {
				return writeLocker.release()
				.then( () => {
					if (writeLocker2) {
						return writeLocker2.release()
						.then(() => { throw error; });
					} else {
						expect(error).to.exist;
						expect(error).to.be.an.instanceof(XError);
						expect(error.code).to.equal(XError.RESOURCE_LOCKED);
						expect(error.message).to.equal('A lock cannot be acquired on the resource: key');
					}
				});
			});
		});

		it('should time out when maxWaitTime is reached', function() {
			this.timeout(5000);
			let writeLocker, writeLocker2;
			return locker.writeLock('key', { maxWaitTime: 0 })
			.then( (rwlock) => {
				writeLocker = rwlock;
				return locker.writeLock('key', { maxWaitTime: 2 });
			}).then( (rwlock) => {
				writeLocker2 = rwlock;
				throw new Error('Should not have locked the second lock');
			})
			.catch( (error) => {
				return writeLocker.release()
				.then( () => {
					if (writeLocker2) {
						return writeLocker2.release()
						.then(() => { throw error; });
					} else {
						expect(error).to.exist;
						expect(error).to.be.an.instanceof(XError);
						expect(error.code).to.equal(XError.RESOURCE_LOCKED);
						expect(error.message).to.equal('Timed out trying to get a resource lock for: key');
					}
				});
			});
		});
	});

	describe('#writeLockSet', function() {
		it('should reject with an error if keys is not an array', function() {
			return locker.writeLockSet('key')
			.catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				expect(error.code).to.equal(XError.INVALID_ARGUMENT);
				expect(error.message).to.equal('keys must be an array');
			});
		});

		it('should return a new lock set with the read locks', function() {
			let returnedLockSet;
			return locker.writeLockSet([ 'key', 'key1' ], { maxWaitTime: 0 })
			.then( (newLockSet) => {
				returnedLockSet = newLockSet;
				expect(newLockSet.locks.key).to.exist;
				expect(newLockSet.locks.key.key).to.equal('key');
				expect(newLockSet.locks.key.isWriteLock).to.be.true;
				expect(newLockSet.locks.key1).to.exist;
				expect(newLockSet.locks.key1.key).to.equal('key1');
				expect(newLockSet.locks.key1.isWriteLock).to.be.true;
				return newLockSet;
			})
			.then( (newLockSet) => {
				return newLockSet.release();
			})
			.catch( (error) => {
				if (returnedLockSet) {
					return returnedLockSet.release().then( () => { throw error; });
				} else {
					throw error;
				}
			});
		});

		it('should return a the given lock set with new read locks appended', function() {
			let returnedLockSet;
			let lockSet = locker.createLockSet();
			return locker.writeLock('key')
			.then( (writeLock) => lockSet.addLock(writeLock))
			.then( () => {
				expect(lockSet.locks.key).to.exist;
				expect(lockSet.locks.key.isWriteLock).to.be.true;
			})
			.then( () => {
				return locker.writeLockSet([ 'key1', 'key2' ], { maxWaitTime: 0, lockSet });
			})
			.then( (lockSet) => {
				expect(lockSet.locks.key).to.exist;
				expect(lockSet.locks.key.key).to.equal('key');
				expect(lockSet.locks.key.isWriteLock).to.be.true;
				expect(lockSet.locks.key1).to.exist;
				expect(lockSet.locks.key1.key).to.equal('key1');
				expect(lockSet.locks.key1.isWriteLock).to.be.true;
				expect(lockSet.locks.key2).to.exist;
				expect(lockSet.locks.key2.key).to.equal('key2');
				expect(lockSet.locks.key2.isWriteLock).to.be.true;
				return lockSet.release();
			})
			.catch( (error) => {
				return lockSet.release()
				.then( () => { throw error; });
			});
		});
	});

	xdescribe('#ReadLockWrap', () => {
		it('should lock a read, run the function and release the key', () => {
			return locker.readLockWrap('key', () => {
				return 1;
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a read, run a promise function that resolves and release the key', () => {
			return locker.readLockWrap('key', (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return resolve(1);
					}, 5);
				});
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a read, run a promise function that resolves and release the key', () => {
			let readLocker;
			return locker.readLockWrap('key', (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return reject(new Error('Error'));
					}, 5);
				});
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				// if you can immediatley lock after error, the key was released
				return locker.readLock('key', { maxWaitTime: 0 }).then( (rwlock) => {
					readLocker = rwlock;
					expect(rwlock).to.exist;
					expect(rwlock.isLocked).to.equal(true);
				}).then( () => {
					return readLocker.release();
				}).catch( (error) => {
					if (readLocker) {
						return readLocker.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
		});

		it('should lock a read, run the function that throws an errors and still release the key', () => {
			let readLocker;
			return locker.readLockWrap('key', () => {
				throw new Error('Oh! No!');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Oh! No!');
				// if you can immediatley lock after error, the key was released
				return locker.readLock('key', { maxWaitTime: 0 }).then( (rwlock) => {
					readLocker = rwlock;
					expect(rwlock).to.exist;
					expect(rwlock.isLocked).to.equal(true);
				}).then( () => {
					return readLocker.release();
				}).then( () => {
					expect(readLocker.isLocked).to.equal(false);
				}).catch( (error) => {
					if (readLocker) {
						return readLocker.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
		});
	});

	xdescribe('#WriteLockWrap', () => {
		it('should lock a write, run the function and release the key', () => {
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, () => {
				return 1;
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a write, run a promise function that rejects and release the key', () => {
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return resolve(1);
					}, 5);
				});
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a write, run a promise function that rejects and release the key', () => {
			let writeLocker;
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return reject(new Error('Error'));
					}, 5);
				});
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				return locker.lock('key', { maxWaitTime: 0 }).then( (rwlock) => {
					writeLocker = rwlock;
					expect(rwlock).to.exist;
					expect(rwlock.isLocked).to.equal(true);
					return writeLocker.release();
				}).then( () => {
					expect(writeLocker.isLocked).to.equal(false);
				}).catch( (error) => {
					if (writeLocker) {
						return writeLocker.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
		});

		it('should lock a write, run the function that errors and still release the key', () => {
			let writeLocker;
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, () => {
				throw new Error('Oh! No!');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Oh! No!');
				return locker.lock('key', { maxWaitTime: 0 }).then( (rwlock) => {
					writeLocker = rwlock;
					expect(rwlock).to.exist;
					expect(rwlock.isLocked).to.equal(true);
					return writeLocker.release();
				}).then( () => {
					expect(writeLocker.isLocked).to.equal(false);
				}).catch( (error) => {
					if (writeLocker) {
						return writeLocker.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
		});
	});

});

xdescribe('Class LockerSet', () => {
	let redizClient, locker, lockSet;
	beforeEach( (done) => {
		redizClient = new RedizClient(REDIZ_CONFIG);
		locker = new Locker(redizClient);
		lockSet = locker.createLockSet();
		done();
	});

	describe('Read Lockers', () => {
		let mockUpgrade;

		afterEach( (done) => {
			if (mockUpgrade) {
				mockUpgrade.restore();
				done();
			} else {
				done();
			}
		});
		it('should add a locker to set after it\'s been created', () => {
			let readLock;
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				lockSet.addLock(rwlock);
				let lock = lockSet.getLock('key');
				expect(lock.keys.length).to.equal(1);
				expect(lock.keys[0]).to.equal('key');
				expect(lock.isWriteLock).to.equal(false);
				expect(lock.isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					return lockSet.release().then( () => {
						throw error;
					});
				} else if (readLock && readLock.isLocked) {
					return readLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it.skip('should create a locker and add it to the set automatically', () => {
			return lockSet.readLock('key').then( () => {
				let lock = lockSet.getLock('key');
				expect(lock.keys.length).to.equal(1);
				expect(lock.keys[0]).to.equal('key');
				expect(lock.isWriteLock).to.equal(false);
				expect(lock.isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it.skip('should create a readLock set and upgrade them all to write lock sets and release them', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				let lock = lockSet.getLock('key');
				expect(lock.isWriteLock).to.equal(false);
				expect(lock.isLocked).to.equal(true);
				return lockSet.upgrade();
			}).then( () => {
				let lock1 = lockSet.getLock('key1');
				expect(lock1.isWriteLock).to.equal(true);
				expect(lock1.isLocked).to.equal(true);
				let lock2 = lockSet.getLock('key2');
				expect(lock2.isWriteLock).to.equal(true);
				expect(lock2.isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it.skip('should create a readlock set and throw an error when the first error occurs', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				mockUpgrade = sinon.mock(lockSet.getLock('key'));
				mockUpgrade.expects('upgrade').once().throws(new Error('Error'));
				return lockSet.upgrade({ onError: 'stop' });
			}).then( () => {
				throw new Error('Upgrade should have errored');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				let lock = lockSet.getLock('key');
				expect(lock.isLocked).to.equal(true);
				mockUpgrade.verify();
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it.skip('should create a readlock set and release them all when one fails to upgrade', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				mockUpgrade = sinon.mock(lockSet.getLock('key'));
				mockUpgrade.expects('upgrade').once().throws(new Error('Error'));
				return lockSet.upgrade({ onError: 'release' });
			}).then( () => {
				throw new Error('Upgrade should have errored');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				mockUpgrade.verify();
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it.skip('should create a readlock set and return all the reads that could not be upgraded', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				expect(lockSet.locks[0]).to.exist;
				mockUpgrade = sinon.mock(lockSet.locks[0]);
				mockUpgrade.expects('upgrade').once().throws(new Error('Error'));
				return lockSet.upgrade({ onError: 'ignore' });
			}).then( (upgradeFailures) => {
				expect(upgradeFailures.length).to.equal(1);
				expect(upgradeFailures[0].keys.length).to.equal(3);
				expect(upgradeFailures[0].keys[0]).to.equal('key');
				expect(upgradeFailures[0].keys[1]).to.equal('key1');
				expect(upgradeFailures[0].keys[2]).to.equal('key2');
				mockUpgrade.verify();
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release( () => {
					throw error;
				});
			});
		});
	});

	describe('Write Lockers', () => {
		it('should add a locker to set after it\'s been created', () => {
			let writeLock;
			return locker.writeLock('key').then( (rwlock) => {
				writeLock = rwlock;
				lockSet.addLock(rwlock);
				expect(lockSet._hasLocks()).to.equal(true);
				let lock = lockSet.getLock('key');
				expect(lock.keys.length).to.equal(1);
				expect(lock.keys[0]).to.equal('key');
				expect(lock.isWriteLock).to.equal(true);
				expect(lock.isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					return lockSet.release().then( () => {
						throw error;
					});
				} else if (writeLock && writeLock.isLocked) {
					return writeLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it.skip('should create a write lock and add it to the set', () => {
			return lockSet.lock('key').then( () => {
				expect(lockSet._hasLocks()).to.equal(true);
				let lock = lockSet.getLock('key');
				expect(lock.keys.length).to.equal(1);
				expect(lock.keys[0]).to.equal('key');
				expect(lock.isWriteLock).to.equal(true);
				expect(lock.isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.equal(false);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});
	});

	describe('Read And Write', () => {
		it.skip('should add reads and writes to the set' +
			' upgrade all the reads to write, and release all of them', () => {
				let lock, lock2, lock4;
				return lockSet.lock([ 'key', 'key1' ]).then( () => {
					return lockSet.writeLock([ 'key2', 'key3' ]);
				}).then( () => {
					return lockSet.readLock([ 'key4', 'key5', 'key6' ]);
				}).then( () => {
					lock = lockSet.getLock('key');
					lock2 = lockSet.getLock('key2');
					lock4 = lockSet.getLock('key4');
					expect(lock.isWriteLock).to.equal(true);
					expect(lock2.isWriteLock).to.equal(true);
					expect(lock4.isWriteLock).to.equal(false);
					return lockSet.upgrade();
				}).then( () => {
					expect(lock4.isWriteLock).to.equal(true);
					return lockSet.release();
				}).then( () => {
					expect(lockSet._hasLocks()).to.equal(false);
				}).catch( (error) => {
					return lockSet.release().then( () => {
						throw error;
					});
				});
			});
	});

	xdescribe('Dependent lock sets', () => {
		let redizClient, locker, lockSet;
		beforeEach( (done) => {
			redizClient = new RedizClient(REDIZ_CONFIG);
			locker = new Locker(redizClient);
			lockSet = locker.createLockSet();
			done();
		});
		it('addDependentLockSet should function, and dependent sets should be cleared on release()', () => {
			let childLockSet = locker.createLockSet();
			return lockSet.lock('key1').then(() => {
				return childLockSet.lock('key2');
			}).then(() => {
				lockSet.addDependentLockSet(childLockSet);
				expect(lockSet._hasLocks()).to.equal(true);
				expect(childLockSet._hasLocks()).to.equal(true);
				return lockSet.release();
			}).then(() => {
				expect(lockSet._hasLocks()).to.equal(false);
				expect(childLockSet._hasLocks()).to.equal(false);
			});
		});
	});
});

