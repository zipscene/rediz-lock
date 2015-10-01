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
			expect(lockSet.locks).to.be.an.instanceof(Array);
			expect(lockSet.locks).to.be.empty;
			expect(lockSet.locker).to.equal(locker);
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
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('key');
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

		it('should lock read for a multiple keys and release them all', () => {
			let readLock;
			return locker.readLock([ 'key', 'key1', 'key2' ],
				{ lockTimeout: 100, maxWaitTime: 20 }).then( (rwlock) => {
					readLock = rwlock;
					expect(rwlock).to.be.an.instanceof(RWLock);
					expect(rwlock.keys.length).to.equal(3);
					expect(rwlock.keys[0]).to.equal('key');
					expect(rwlock.keys[1]).to.equal('key1');
					expect(rwlock.keys[2]).to.equal('key2');
					expect(rwlock.locker).to.equal(locker);
					expect(rwlock.isWriteLock).to.equal(false);
					expect(rwlock.isLocked).to.equal(true);
					return rwlock.release();
				}).then( () => {
					expect(readLock.isLocked).to.equal(false);
					expect(readLock.keys[0]).to.equal('key');
					expect(readLock.keys[1]).to.equal('key1');
					expect(readLock.keys[2]).to.equal('key2');
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
							// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
							expect(error.message).to.equal('Resource Locked');
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
							// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
							expect(error.message).to.equal('Time out waiting for lock: key');
						}
					});
				} else {
					throw error;
				}
			});
		});

		it('should lock all reads, and then upgrade them to writes', () => {
			let readLock;
			return locker.readLock([ 'key', 'key1' ]).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.exist;
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.keys[0]).to.equal('key');
				expect(rwlock.keys[1]).to.equal('key1');
				expect(rwlock.isWriteLock).to.equal(false);
				return rwlock.upgrade();
			}).then( () => {
				expect(readLock.isWriteLock).to.equal(true);
				expect(readLock.tokens.length).to.equal(2);
				expect(readLock.keys.length).to.equal(2);
				expect(readLock.keys[0]).to.equal('key');
				expect(readLock.keys[1]).to.equal('key1');
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
			return locker.readLock([ 'key', 'key1' ]).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.exist;
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.keys[0]).to.equal('key');
				expect(rwlock.keys[1]).to.equal('key1');
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
					expect(readLock.keys[0]).to.equal('key');
				}
			});
		});
	});

	describe('#writeLock', () => {

		it('should lock write for a single key', () => {
			let writeLocker;
			return locker.writeLock('key').then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.tokens.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('key');
				expect(rwlock.locker).to.equal(locker);
				return rwlock.release();
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

		it('should create a lock for multiple keys', () => {
			let writeLocker;
			return locker.writeLock([ 'key', 'key1' ]).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.tokens.length).to.equal(2);
				return rwlock.release();
			}).then( () => {
				expect(writeLocker.isLocked).to.equal(false);
				expect(writeLocker.keys[0]).to.equal('key');
				expect(writeLocker.keys[1]).to.equal('key1');
			}).catch( (error) => {
				return writeLocker.release().then( () => {
					throw error;
				});
			});
		});

		it('should fail when trying to lock a previously locked write locker', () => {
			let writeLocker;
			return locker.writeLock([ 'key', 'key' ], { maxWaitTime: 0 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLocker) {
					return writeLocker.release().then( () => {
						throw error;
					});
				} else {
					expect(error).to.exist;
					expect(error).to.be.an.instanceof(XError);
					// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
					expect(error.message).to.equal('Resource Locked');
				}
			});
		});

		it('should time out when maxWaitTime is reached', function() {
			this.timeout(5000);
			let writeLocker;
			return locker.writeLock([ 'key', 'key' ], { maxWaitTime: 2 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLocker) {
					return writeLocker.release().then( () => {
						throw error;
					});
				} else {
					expect(error).to.exist;
					expect(error).to.be.an.instanceof(XError);
					// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
					expect(error.message).to.equal('Time out waiting for lock: key');
				}
			});
		});
	});

	describe('#ReadLockWrap', () => {
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

	describe('#WriteLockWrap', () => {
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

describe('Class LockerSet', () => {
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
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('key');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				console.log(error);
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

		it('should create a locker and add it to the set automatically', () => {
			return lockSet.readLock('key').then( () => {
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('key');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it('should create a readLock set and upgrade them all to write lock sets and release them', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				expect(lockSet.locks[0].keys.length).to.equal(3);
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.upgrade();
			}).then( () => {
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].keys[0]).to.equal('key');
				expect(lockSet.locks[0].keys[1]).to.equal('key1');
				expect(lockSet.locks[0].keys[2]).to.equal('key2');
				expect(lockSet.locks[0].tokens.length).to.equal(3);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it('should create a readlock set and throw an error when the first error occurs', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				expect(lockSet.locks[0]).to.exist;
				mockUpgrade = sinon.mock(lockSet.locks[0]);
				mockUpgrade.expects('upgrade').once().throws(new Error('Error'));
				return lockSet.upgrade({ onError: 'stop' });
			}).then( () => {
				throw new Error('Upgrade should have errored');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				expect(lockSet.locks[0].isLocked).to.equal(true);
				expect(lockSet.locks.length).to.equal(1);
				mockUpgrade.verify();
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it('should create a readlock set and release them all when one fails to upgrade', () => {
			return lockSet.readLock([ 'key', 'key1', 'key2' ]).then( () => {
				expect(lockSet.locks[0]).to.exist;
				mockUpgrade = sinon.mock(lockSet.locks[0]);
				mockUpgrade.expects('upgrade').once().throws(new Error('Error'));
				return lockSet.upgrade({ onError: 'release' });
			}).then( () => {
				throw new Error('Upgrade should have errored');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Error');
				mockUpgrade.verify();
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});

		it('should create a readlock set and return all the reads that could not be upgraded', () => {
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
				expect(lockSet.locks.length).to.equal(0);
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
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('key');
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
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

		it('should create a write lock and add it to the set', () => {
			return lockSet.lock('key').then( () => {
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('key');
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks.length).to.equal(0);
			}).catch( (error) => {
				return lockSet.release().then( () => {
					throw error;
				});
			});
		});
	});

	describe('Read And Write', () => {
		it('should add reads and writes to the set' +
			' upgrade all the reads to write, and release all of them', () => {
				return lockSet.lock([ 'key', 'key1' ]).then( () => {
					expect(lockSet.locks.length).to.equal(1);
					expect(lockSet.locks[0].keys[0]).to.equal('key');
					expect(lockSet.locks[0].keys[1]).to.equal('key1');
					return lockSet.writeLock([ 'key2', 'key3' ]);
				}).then( () => {
					expect(lockSet.locks.length).to.equal(2);
					expect(lockSet.locks[1].keys.length).to.equal(2);
					return lockSet.readLock([ 'key4', 'key5', 'key6' ]);
				}).then( () => {
					expect(lockSet.locks.length).to.equal(3);
					expect(lockSet.locks[2].keys.length).to.equal(3);
					expect(lockSet.locks[0].isWriteLock).to.equal(true);
					expect(lockSet.locks[1].isWriteLock).to.equal(true);
					expect(lockSet.locks[2].isWriteLock).to.equal(false);
					return lockSet.upgrade();
				}).then( () => {
					expect(lockSet.locks.length).to.equal(3);
					expect(lockSet.locks[2].keys[0]).to.equal('key4');
					expect(lockSet.locks[2].keys[1]).to.equal('key5');
					expect(lockSet.locks[2].keys[2]).to.equal('key6');
					expect(lockSet.locks[2].isWriteLock).to.equal(true);
					return lockSet.release();
				}).then( () => {
					expect(lockSet.locks.length).to.equal(0);
				}).catch( (error) => {
					return lockSet.release().then( () => {
						throw error;
					});
				});
			});
	});
});

