local numKeyArgs = 2

-- Arguments:
-- Keys: <WriteLockKey>, <ReadLockKey>
-- Params: <LockToken> <KeyTimeout>
-- Returns:
-- { 0, <Token> } - Key is already write locked, returning token that owns the write lock
-- { 1, <TokenList> } - Key was successfully read locked, also returns the list of holders

local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock then
	return { 0, existingWriteLock }
else
	redis.call("sadd", KEYS[2], ARGV[1])
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("expire", KEYS[2], ARGV[2])
	end
	local holders = redis.call("smembers", KEYS[2])
	return { 1, holders }
end

