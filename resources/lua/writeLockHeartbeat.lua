local numKeyArgs = 1

-- Arguments:
-- Keys: <WriteLockKey>
-- Params: <LockToken> <Expiry>
-- Returns:
-- { 0, <Token> } - Write lock has been claimed by someone else
-- { 1 } - Heartbeat successful
-- { 3 } - Lock key expired, lock no longer owned

local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock then
	if existingWriteLock == ARGV[1] then
		redis.call("expire", KEYS[1], ARGV[2])
		return { 1 }
	else
		return { 0, existingWriteLock }
	end
else
	return { 3 }
end

