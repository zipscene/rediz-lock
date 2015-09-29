const expect = require('chai').expect;
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

		it('should lock read for a single key and release it', () => {
			let readLock;
			return locker.readLock('myKey').then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('myKey');
				expect(rwlock.locker).to.equal(locker);
				return rwlock.release();
			}).then( () => {
				expect(readLock.isLocked).to.equal(false);
			}).catch( (error) => {
				if (readLock) {
					readLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should lock read for a multiple keys and release them all', () => {
			let readLock;
			return locker.readLock([ 'key1', 'key2', 'key3' ],
				{ lockTimeout: 100, maxWaitTime: 20 }).then( (rwlock) => {
					readLock = rwlock;
					expect(rwlock).to.be.an.instanceof(RWLock);
					expect(rwlock.keys.length).to.equal(3);
					expect(rwlock.keys[0]).to.equal('key1');
					expect(rwlock.keys[1]).to.equal('key2');
					expect(rwlock.keys[2]).to.equal('key3');
					expect(rwlock.locker).to.equal(locker);
					expect(rwlock.isWriteLock).to.equal(false);
					expect(rwlock.isLocked).to.equal(true);
					return rwlock.release();
				}).then( () => {
					expect(readLock.isLocked).to.equal(false);
				}).catch( (error) => {
					if (readLock) {
						readLock.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
		});

		it('should time out when trying to access an already read locked key', () => {
			let readLock;
			return locker.readLock([ 'key1', 'key1' ], { maxWaitTime: 0, lockTimeout: 12000000 }).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (readLock) {
					readLock.release().then( () => {
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
			let readLock;
			return locker.readLock([ 'key1', 'key1' ], { maxWaitTime: 2, lockTimeout: 12000000 }).then( (rwlock) => {
				readLock = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (readLock) {
					readLock.release().then( () => {
						throw error;
					});
				} else {
					expect(error).to.exist;
					expect(error).to.be.an.instanceof(XError);
					// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
					expect(error.message).to.equal('Time out waiting for lock: key1');
				}
			});
		});
	});

	describe('#writeLock', () => {

		it('should lock write for a single key', () => {
			let writeLocker;
			return locker.writeLock('writeKey', { lockTimeout: 1 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.tokens.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('writeKey');
				expect(rwlock.locker).to.equal(locker);
				return rwlock.release();
			}).then( () => {
				expect(writeLocker.isLocked).to.equal(false);
			}).catch( (error) => {
				if (writeLocker) {
					writeLocker.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should create a lock for multiple keys', () => {
			let writeLocker;
			return locker.writeLock([ 'writeKey1', 'writeKey2' ], { lockTimeout: 1 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.tokens.length).to.equal(2);
				return rwlock.release();
			}).then( () => {
				expect(writeLocker.isLocked).to.equal(false);
			}).catch( (error) => {
				if (writeLocker) {
					writeLocker.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should fail when trying to lock a previously locked write locker', () => {
			let writeLocker;
			return locker.writeLock([ 'wKey1', 'wKey1' ], { maxWaitTime: 0 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLocker) {
					writeLocker.release().then( () => {
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
			return locker.readLock([ 'wKey1', 'wKey1' ], { maxWaitTime: 2, lockTimeout: 12000000 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				if (writeLocker) {
					writeLocker.release().then( () => {
						throw error;
					});
				} else {
					expect(error).to.exist;
					expect(error).to.be.an.instanceof(XError);
					// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
					expect(error.message).to.equal('Time out waiting for lock: wKey1');
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
			return locker.readLockWrap('key', (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return reject(new Error());
					}, 5);
				});
			}).catch( (error) => {
				expect(error).to.exist;
			});
		});

		it('should lock a read, run the function that throws an errors and still release the key', () => {
			return locker.readLockWrap('key', () => {
				throw new Error('Oh! No!');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Oh! No!');
			});
		});
	});

	describe('#WriteLockWrap', () => {
		it('should lock a write, run the function and release the key', () => {
			return locker.writeLockWrap('key', () => {
				return 1;
			}).then( (result) => {
				expect(result).to.equal(1);
			});
		});

		it('should lock a write, run a promise function that rejects and release the key', () => {
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

		it('should lock a write, run a promise function that rejects and release the key', () => {
			return locker.readLockWrap('key', (callback) => {
				return new Promise( (resolve, reject) => {
					setTimeout( () => {
						return reject(new Error());
					}, 5);
				});
			}).catch( (error) => {
				expect(error).to.exist;
			});
		});

		it('should lock a write, run the function that errors and still release the key', () => {
			return locker.writeLockWrap('key', () => {
				throw new Error('Oh! No!');
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error.message).to.equal('Oh! No!');
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
		it('should add a locker to set after it\'s been created', function() {
			this.timeout(5000);
			let readLock;
			return locker.readLock('setKey').then( (rwlock) => {
				readLock = rwlock;
				lockSet.addLock(rwlock);
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('setKey');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					lockSet.release().then( () => {
						throw error;
					});
				} else if (readLock && readLock.isLocked) {
					readLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should create a locker and add it to the set automatically', function() {
			this.timeout(5000);
			return lockSet.readLock('setKey2').then( () => {
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('setKey2');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					lockSet.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should create a readLock set and upgrade them all to write lock sets and release them', function() {
			this.timeout(5000);
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
				expect(lockSet.locks[0].isLocked).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					lockSet.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});
	});

	describe('Write Lockers', () => {
		it('should add a locker to set after it\'s been created', function() {
			this.timeout(5000);
			let writeLock;
			return locker.writeLock('write-key-set').then( (rwlock) => {
				writeLock = rwlock;
				lockSet.addLock(rwlock);
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('write-key-set');
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					lockSet.release().then( () => {
						throw error;
					});
				} else if (writeLock && writeLock.isLocked) {
					writeLock.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});

		it('should create a write lock and add it to the set', function() {
			this.timeout(5000);
			return lockSet.lock('write-key-set1').then( () => {
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('write-key-set1');
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			}).catch( (error) => {
				if (lockSet.locks.length) {
					lockSet.release().then( () => {
						throw error;
					});
				} else {
					throw error;
				}
			});
		});
	});

	describe('Read And Write', () => {
		it('should add reads and writes to the set' +
			' upgrade all the reads to write, and release all of them', function() {
				this.timeout(10000);
				return lockSet.lock([ 'write1', 'write2' ]).then( () => {
					expect(lockSet.locks.length).to.equal(1);
					expect(lockSet.locks[0].keys[0]).to.equal('write1');
					expect(lockSet.locks[0].keys[1]).to.equal('write2');
					return lockSet.writeLock([ 'write3', 'write4' ]);
				}).then( () => {
					expect(lockSet.locks.length).to.equal(2);
					expect(lockSet.locks[1].keys.length).to.equal(2);
					return lockSet.readLock([ 'read1', 'read2', 'read3' ]);
				}).then( () => {
					expect(lockSet.locks.length).to.equal(3);
					expect(lockSet.locks[2].keys.length).to.equal(3);
					expect(lockSet.locks[0].isWriteLock).to.equal(true);
					expect(lockSet.locks[1].isWriteLock).to.equal(true);
					expect(lockSet.locks[2].isWriteLock).to.equal(false);
					return lockSet.upgrade();
				}).then( () => {
					expect(lockSet.locks.length).to.equal(3);
					expect(lockSet.locks[2].keys[0]).to.equal('read1');
					expect(lockSet.locks[2].keys[1]).to.equal('read2');
					expect(lockSet.locks[2].keys[2]).to.equal('read3');
					expect(lockSet.locks[2].isWriteLock).to.equal(true);
					return lockSet.release();
				}).then( () => {
					expect(lockSet.locks.length).to.equal(3);
					expect(lockSet.locks[0].keys[0]).to.equal('write1');
					expect(lockSet.locks[0].keys[1]).to.equal('write2');
					expect(lockSet.locks[1].keys[0]).to.equal('write3');
					expect(lockSet.locks[1].keys[1]).to.equal('write4');
					expect(lockSet.locks[2].keys[0]).to.equal('read1');
					expect(lockSet.locks[2].keys[1]).to.equal('read2');
					expect(lockSet.locks[2].keys[2]).to.equal('read3');
					expect(lockSet.locks[0].isLocked).to.equal(false);
					expect(lockSet.locks[1].isLocked).to.equal(false);
					expect(lockSet.locks[2].isLocked).to.equal(false);
				}).catch( (error) => {
					if (lockSet.locks.length) {
						lockSet.release().then( () => {
							throw error;
						});
					} else {
						throw error;
					}
				});
			});
	});
});

