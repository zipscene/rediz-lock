local numKeyArgs = 1

-- Arguments:
-- Keys: <ReadLockKey>
-- Params: <LockToken>
-- Returns:
-- { 1, <TokenList> } - Read lock released, returns remaining holders

redis.call("srem", KEYS[1], ARGV[1])
return { 1, redis.call("smembers", KEYS[1]) }

