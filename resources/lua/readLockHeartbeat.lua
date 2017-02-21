local numKeyArgs = 1
local existingReadLock = redis.call("get", KEYS[1])
if existingReadLock then
	redis.call("expire", KEYS[1], ARGV[1]);
	return { 1 }
else
	return { 3 }
end

