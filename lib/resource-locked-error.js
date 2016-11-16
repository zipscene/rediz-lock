// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const XError = require('xerror');

/**
 * Error when a key is locked and cannot be acquired.
 *
 * @class ResourceLockedError
 * @constructor
 */
class ResourceLockedError extends XError {

	constructor(lockKey, message) {
		let msg = message || 'A lock cannot be acquired on the resource: ' + lockKey;
		super(XError.RESOURCE_LOCKED, msg, { key: lockKey });
	}

}

// Register the XError code with default message
XError.registerErrorCode('resource_locked', {
	message: 'A lock cannot be acquired on a resource',
	http: 500
});

module.exports = ResourceLockedError;
