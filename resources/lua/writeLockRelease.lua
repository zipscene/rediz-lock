local numKeyArgs = 1

-- Arguments:
-- Keys: <WriteLockKey>
-- Params: <LockToken>
-- Returns:
-- { 0 } - Lock was previously released or expired
-- { 1 } - Lock released

if redis.call("get", KEYS[1]) == ARGV[1] then
	redis.call("del", KEYS[1])
	return { 1 }
else
	return { 0 }
end
