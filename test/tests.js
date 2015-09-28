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
		it('should lock read for a single key', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			return locker.readLock('myKey').then( (rwlock) => {
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('myKey');
				expect(rwlock.locker).to.equal(locker);
			});
		});

		it('should lock read for a multiple keys', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			return locker.readLock([ 'key1', 'key2' ], { lockTimeout: 100, maxWaitTime: 20 }).then( (rwlock) => {
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(2);
				expect(rwlock.keys[0]).to.equal('key1');
				expect(rwlock.keys[1]).to.equal('key2');
				expect(rwlock.locker).to.equal(locker);
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
		it('should lock write for a single key', () => {
			let redizClient = new RedizClient(REDIZ_CONFIG);
			let locker = new Locker(redizClient);
			return locker.writeLock('key').then( (rwlock) => {
				expect(rwlock).to.be.an.instanceof(RWLock);
				expect(rwlock.keys.length).to.equal(1);
				expect(rwlock.keys[0]).to.equal('key');
				expect(rwlock.locker).to.equal(locker);
			});
		});
	});

});


