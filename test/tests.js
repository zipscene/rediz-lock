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
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
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
				console.error(error);
				throw error;
			});
		});

		it('should lock read for a multiple keys and release them all', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
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
					console.error(error);
					throw error;
				});
		});

		it('should time out when trying to access an already read locked key', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			return locker.readLock([ 'key1', 'key1' ], { maxWaitTime: 0 }).then( (rwlock) => {
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
				expect(error.message).to.equal('Resource Locked');
			});
		});

		it('should time out when maxWaitTime is reached', function() {
			this.timeout(5000);
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			return locker.readLock([ 'key1', 'key1' ], { maxWaitTime: 2 }).then( (rwlock) => {
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
				expect(error.message).to.equal('Time out waiting for lock: key1');
			});
		});
	});

	describe('#writeLock', () => {
		let redizClient, locker;
		beforeEach( (done) => {
			redizClient = new RedizClient(REDIZ_CONFIG);
			locker = new Locker(redizClient);
			done();
		});

		it('should lock write for a single key', function() {
			this.timeout(5000);
			let writeLocker;
			return locker.writeLock('writeKey', { lockTimeout: 1 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.tokens.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('writeKey');
				expect(rwlock.locker).to.equal(locker);
				console.log('here!!');
				return rwlock.release();
			}).then( () => {
				console.log('here!!');
				expect(writeLocker.isLocked).to.equal(false);
			});
		});

		it('should create a lock for multiple keys', function() {
			this.timeout(5000);
			let writeLocker;
			return locker.writeLock([ 'writeKey1', 'writeKey2' ], { lockTimeout: 1 }).then( (rwlock) => {
				writeLocker = rwlock;
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.tokens.length).to.equal(2);
				return rwlock.release();
			}).then( () => {
				expect(writeLocker.isLocked).to.equal(false);
			});
		});

		it('should fail when trying to lock a previously locked write locker', () => {
			return locker.writeLock([ 'wKey1', 'wKey1' ], { maxWaitTime: 0 }).then( (rwlock) => {
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
				expect(error.message).to.equal('Resource Locked');
			});
		});

		it('should time out when maxWaitTime is reached', function() {
			this.timeout(5000);
			return locker.readLock([ 'wKey1', 'wKey1' ], { maxWaitTime: 2 }).then( (rwlock) => {
				expect(rwlock).to.not.exist;
			}).catch( (error) => {
				expect(error).to.exist;
				expect(error).to.be.an.instanceof(XError);
				// expect(error.code).to.equal(XError.RESOURCE_LOCKED);
				expect(error.message).to.equal('Time out waiting for lock: wKey1');
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
		it('should add a locker to set after it\'s been created', () => {
			return locker.readLock('setKey').then( (rwlock) => {
				lockSet.addLock(rwlock);
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('setKey');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			});
		});

		it('should create a locker and add it to the set automatically', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			let lockSet = locker.createLockSet();
			return lockSet.readLock('setKey2').then( () => {
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('setKey2');
				expect(lockSet.locks[0].isWriteLock).to.equal(false);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			});
		});
	});

	describe('Write Lockers', () => {
		it('should add a locker to set after it\'s been created', () => {
			return locker.writeLock('setKey').then( (rwlock) => {
				lockSet.addLock(rwlock);
				expect(lockSet.locks.length).to.equal(1);
				expect(lockSet.locks[0].keys.length).to.equal(1);
				expect(lockSet.locks[0].keys[0]).to.equal('setKey');
				expect(lockSet.locks[0].isWriteLock).to.equal(true);
				expect(lockSet.locks[0].isLocked).to.equal(true);
				return lockSet.release();
			}).then( () => {
				expect(lockSet.locks[0].isLocked).to.equal(false);
			});
		});
	});
});

