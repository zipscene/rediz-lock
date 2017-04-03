local numKeyArgs = 2

-- Arguments:
-- Keys: <WriteLockKey> <ReadLockKey>
-- Params: <LockToken> <Expiry>
-- Returns:
-- { 0, <Token> } - A write lock has been claimed by someone else (<Token>)
-- { 1 } - Write lock successfully claimed and owned
-- { 2, <TokenList> } - Write lock claimed, but there's an existing read lock, so we don't own it yet

local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock then
	return { 0, existingWriteLock }
else
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[2])
	else
		redis.call("set", KEYS[1], ARGV[1])
	end
	if redis.call("scard", KEYS[2]) > 0 then
		return { 2, redis.call("smembers", KEYS[2]) }
	else
		return { 1 }
	end
end
