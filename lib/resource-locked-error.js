const XError = require('xerror');

/**
 * Error when a key is locked and cannot be acquired.
 *
 * @class ResourceLockedError
 * @constructor
 */
class ResourceLockedError extends XError {

	constructor(lockKey) {
		super(XError.RESOURCE_LOCKED, 'A lock cannot be acquired on the resource: ' + lockKey, { key: lockKey });
	}

}

// Register the XError code with default message
XError.registerErrorCode('resource_locked', {
	message: 'A lock cannot be acquired on a resource',
	http: 500
});

module.exports = ResourceLockedError;
