local numKeyArgs = 1

-- Arguments:
-- Keys: <ReadLockKey>
-- Params: <LockToken> <Expiry>
-- Returns:
-- { 1 } - Lock still held, heartbeat successful
-- { 3 } - Lost the lock

local existingReadLock = redis.call("scard", KEYS[1])
if existingReadLock > 0 then
	if redis.call("sismember", KEYS[1], ARGV[1]) then
		redis.call("expire", KEYS[1], ARGV[2]);
		return { 1 }
	else
		return { 3 }
	end
else
	return { 3 }
end

