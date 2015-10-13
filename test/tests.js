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

describe('Class Locker', function() {
	let redizClient, locker;
	beforeEach( function(done) {
		redizClient = new RedizClient(REDIZ_CONFIG);
		locker = new Locker(redizClient);
		done();
	});

	it('construct a locker with a redis client, and a scriptWaiter', function() {
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

	describe('#createLockSet', function() {
		it('should create a new lock set instance', function(done) {
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

	describe('#readLock', function() {
		let writeLockMock;
		after( function(done) {
			if (writeLockMock) {
				writeLockMock.restore();
				done();
			} else {
				done();
			}
		});

		it('should lock read for a single key and release it', function() {
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

		it('should time out when trying to access an already read locked key', function() {
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

		it('should lock a read, and then upgrade them to writes', function() {
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

		it('should attempts to upgrade, fails and releases all the locks', function() {
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
				throw new Error('This should have thrown an error');
			}).catch( (error) => {
				if (readLock.isLocked) {
					return readLock.release().then( () => {
						throw error;
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
				return newLockSet.release();
			})
			.then( () => {
				expect(returnedLockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				if (returnedLockSet) {
					return returnedLockSet.release().then( () => { throw error; });
				} else {
					throw error;
				}
			});
		});

		it('should return a lock set with one lock when trying to lock the same key', function() {
			let returnedLockSet;
			return locker.readLockSet([ 'key', 'key' ], { maxWaitTime: 0 })
			.then( (newLockSet) => {
				returnedLockSet = newLockSet;
				expect(newLockSet._hasLocks()).to.be.true;
				expect(Object.keys(newLockSet.locks).length).to.equal(1);
				let lock = newLockSet.getLock('key');
				expect(lock).to.exist;
				expect(lock).to.be.an.instanceof(RWLock);
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.false;
				return newLockSet.release();
			})
			.then( () => {
				expect(returnedLockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				if (returnedLockSet) {
					return returnedLockSet.release().then( () => { throw error; });
				} else {
					throw error;
				}
			});
		});

		it('should not add the new read lock to a lock set that already contains a lock of that same key', function() {
			let readLock;
			let lockSet = locker.createLockSet();
			return locker.readLock('key', { maxWaitTime: 0 })
			.then( (readLock) => {
				expect(lockSet._hasLocks()).to.be.false;
				lockSet.addLock(readLock);
				expect(lockSet._hasLocks()).to.be.true;
			})
			.then( () => {
				return locker.readLockSet([ 'key', 'key1' ], { lockSet });
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.true;
				expect(Object.keys(lockSet.locks).length).to.equal(2);
				return lockSet.release();
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				return lockSet.release()
				.then( () => {
					if (readLock) {
						return readLock.release();
					}
				})
				.then( () => { throw error; });
			});
		});

		it('should return a the given lock set with new read locks appended', function() {
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

		it('should release all the keys and throw an error if one of the lock scripts fails', function() {
			let lockSet;
			let readLockMock = sinon.mock(locker);
			readLockMock.expects('readLock').once().throws(new XError(XError.INTERNAL_ERROR));
			return locker.readLockSet([ 'key', 'key1' ])
			.then( (lockSet) => {
				console.log(lockSet);
				return lockSet.release();
			})
			.then( () => {
				throw new Error('Should have not returned a lock set');
			})
			.catch( (error) => {
				expect(error).to.be.an.instanceof(XError);
				expect(error.code).to.equal(XError.INTERNAL_ERROR);
				readLockMock.verify();
				readLockMock.restore();
				return locker.readLockSet([ 'key', 'key1' ])
				.then( (set) => {
					lockSet = set;
					expect(set._hasLocks()).to.be.true;
					return set.release();
				})
				.then( () => {
					expect(lockSet._hasLocks()).to.be.false;
				})
				.catch( (error) => {
					if (lockSet) {
						return lockSet.release()
						.then( () => { throw error; });
					} else {
						throw error;
					}
				});
			});
		});
	});

	describe('#writeLock', function() {

		it('should lock write for a single key', function() {
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

		it('should fail when trying to lock a previously locked write locker', function() {
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

		it('should return a lock set with one lock when trying to lock the same key', function() {
			let returnedLockSet;
			return locker.writeLockSet([ 'key', 'key' ], { maxWaitTime: 0 })
			.then( (newLockSet) => {
				returnedLockSet = newLockSet;
				expect(newLockSet._hasLocks()).to.be.true;
				expect(Object.keys(newLockSet.locks).length).to.equal(1);
				let lock = newLockSet.getLock('key');
				expect(lock).to.exist;
				expect(lock).to.be.an.instanceof(RWLock);
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.true;
				return newLockSet.release();
			})
			.then( () => {
				expect(returnedLockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				if (returnedLockSet) {
					return returnedLockSet.release().then( () => { throw error; });
				} else {
					throw error;
				}
			});
		});

		it('should not add the new read lock to a lock set that already contains a lock of that same key', function() {
			let writeLock;
			let lockSet = locker.createLockSet();
			return locker.writeLock('key', { maxWaitTime: 0 })
			.then( (writeLock) => {
				expect(lockSet._hasLocks()).to.be.false;
				lockSet.addLock(writeLock);
				expect(lockSet._hasLocks()).to.be.true;
			})
			.then( () => {
				return locker.writeLockSet([ 'key', 'key1' ], { lockSet });
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.true;
				expect(Object.keys(lockSet.locks).length).to.equal(2);
				return lockSet.release();
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				return lockSet.release()
				.then( () => {
					if (writeLock) {
						return writeLock.release();
					}
				})
				.then( () => { throw error; });
			});
		});

		it('should return a the given lock set with new read locks appended', function() {
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

		it('should release all the keys and throw an error if one of the lock scripts fails', function() {
			let lockSet;
			let writeLockMock = sinon.mock(locker);
			writeLockMock.expects('writeLock').once().throws(new XError(XError.INTERNAL_ERROR));
			return locker.writeLockSet([ 'key', 'key1' ])
			.then( (lockSet) => {
				return lockSet.release();
			})
			.then( () => {
				throw new Error('Should have not returned a lock set');
			})
			.catch( (error) => {
				expect(error).to.be.an.instanceof(XError);
				expect(error.code).to.equal(XError.INTERNAL_ERROR);
				writeLockMock.verify();
				writeLockMock.restore();
				return locker.writeLockSet([ 'key', 'key1' ])
				.then( (set) => {
					lockSet = set;
					expect(set._hasLocks()).to.be.true;
					return set.release();
				})
				.then( () => {
					expect(lockSet._hasLocks()).to.be.false;
				})
				.catch( (error) => {
					if (lockSet) {
						return lockSet.release()
						.then( () => { throw error; });
					} else {
						throw error;
					}
				});
			});
		});
	});

	describe('#ReadLockWrap', function() {
		it('should lock a read, run the function and release the key', function() {
			return locker.readLockWrap('key', () => {
				return 1;
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a read, run a promise function that resolves and release the key', function() {
			return locker.readLockWrap('key', () => {
				return new Promise( (resolve) => {
					setTimeout( () => {
						return resolve(1);
					}, 5);
				});
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a read, run a promise function that resolves and release the key', function() {
			let readLocker;
			return locker.readLockWrap('key', () => {
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

		it('should lock a read, run the function that throws an errors and still release the key', function() {
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

	describe('#WriteLockWrap', function() {
		it('should lock a write, run the function and release the key', function() {
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, () => {
				return 1;
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a write, run a promise function that rejects and release the key', function() {
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, () => {
				return new Promise( (resolve) => {
					setTimeout( () => {
						return resolve(1);
					}, 5);
				});
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a write, run a promise function that rejects and release the key', function() {
			let writeLocker;
			return locker.writeLockWrap('key', { maxWaitTime: 0 }, () => {
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

		it('should lock a write, run the function that errors and still release the key', function() {
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

describe('Class LockerSet', function() {
	let redizClient, locker, lockSet;

	beforeEach( function(done) {
		redizClient = new RedizClient(REDIZ_CONFIG);
		locker = new Locker(redizClient);
		lockSet = locker.createLockSet();
		done();
	});

	describe('Read Lockers', function() {
		let mockUpgrade;

		afterEach( function(done) {
			if (mockUpgrade) {
				mockUpgrade.restore();
				done();
			} else {
				done();
			}
		});
		it('should add a locker to set after it\'s been created', function() {
			let readLock;
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				lockSet.addLock(rwlock);
				let lock = lockSet.getLock('key');
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.false;
				expect(lock.isLocked).to.be.true;
				return lockSet.release();
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				return lockSet.release().then( () => {
					if (readLock && readLock.isLocked) {
						return readLock.release().then( () => { throw error; });
					} else {
						throw error;
					}
				});
			});
		});

		it('should create a lock set of reads and then upgrade them to write locks', function() {
			let readLock;
			return locker.readLock('key').then( (rwlock) => {
				readLock = rwlock;
				lockSet.addLock(rwlock);
				let lock = lockSet.getLock('key');
				expect(lock).to.exist;
				expect(lock).to.be.an.instanceof(RWLock);
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.false;
				expect(lock.isLocked).to.be.true;
				return lockSet.upgrade();
			})
			.then( () => {
				let lock = lockSet.getLock('key');
				expect(lock).to.exist;
				expect(lock).to.be.an.instanceof(RWLock);
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.true;
				expect(lock.isLocked).to.be.true;
				expect(lockSet._hasLocks()).to.be.true;
				return lockSet.release();
			})
			.then( () => {
				expect(lockSet._hasLocks()).to.be.false;
			})
			.catch( (error) => {
				return lockSet.release().then( () => {
					if (readLock && readLock.isLocked) {
						return readLock.release().then( () => { throw error; });
					} else {
						throw error;
					}
				});
			});
		});

	});

	describe('Write Lockers', function() {
		it('should add a locker to set after it\'s been created', function() {
			let writeLock;
			return locker.writeLock('key').then( (rwlock) => {
				writeLock = rwlock;
				lockSet.addLock(rwlock);
				expect(lockSet._hasLocks()).to.be.true;
				let lock = lockSet.getLock('key');
				expect(lock.key).to.equal('key');
				expect(lock.isWriteLock).to.be.true;
				expect(lock.isLocked).to.be.true;
				return lockSet.release();
			}).then( () => {
				expect(lockSet._hasLocks()).to.be.false;
			}).catch( (error) => {
				return lockSet.release().then( () => {
					if (writeLock && writeLock.isLocked) {
						return writeLock.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
		});
	});

	describe('Read And Write', function() {
		it('should add reads and writes to the set' +
			' upgrade all the reads to write, and release all of them', function() {
				let writeLock, writeLock1, readLock, lock, lock1, lock2;
				return locker.lock('key').then( (_writeLock) => {
					writeLock = _writeLock;
					lockSet.addLock(_writeLock);
					expect(lockSet._hasLocks()).to.be.true;
					return locker.writeLock('key1');
				}).then( (_writeLock) => {
					writeLock1 = _writeLock;
					lockSet.addLock(_writeLock);
					expect(lockSet._hasLocks()).to.be.true;
					return locker.readLock('key2');
				}).then( (_readLock) => {
					readLock = _readLock;
					lockSet.addLock(_readLock);
					expect(lockSet._hasLocks()).to.be.true;
					lock = lockSet.getLock('key');
					lock1 = lockSet.getLock('key1');
					lock2 = lockSet.getLock('key2');
					expect(lock.isWriteLock).to.be.true;
					expect(lock1.isWriteLock).to.be.true;
					expect(lock2.isWriteLock).to.be.false;
					return lockSet.upgrade();
				}).then( () => {
					expect(lockSet._hasLocks()).to.be.true;
					expect(lock2.isWriteLock).to.be.true;
					expect(lock2.isLocked).to.be.true;
					return lockSet.release();
				}).then( () => {
					expect(lockSet._hasLocks()).to.be.false;
				}).catch( (error) => {
					return lockSet.release()
					.then( () => {
						if (writeLock) {
							return writeLock.release();
						}
					})
					.then( () => {
						if (writeLock1) {
							return writeLock1.release();
						}
					})
					.then( () => {
						if (readLock) {
							return readLock.release();
						}
					})
					.then( () => {
						throw error;
					});
				});
			});
	});

	describe('Dependent lock sets', function() {
		let redizClient, locker, lockSet;
		beforeEach( function(done) {
			redizClient = new RedizClient(REDIZ_CONFIG);
			locker = new Locker(redizClient);
			lockSet = locker.createLockSet();
			done();
		});
		it('addDependentLockSet should function, and dependent sets should be cleared on release()', function() {
			let childLockSet = locker.createLockSet();
			return locker.lock('key1').then((writeLock) => {
				lockSet.addLock(writeLock);
				return locker.lock('key2');
			}).then((writeLock) => {
				childLockSet.addLock(writeLock);
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

