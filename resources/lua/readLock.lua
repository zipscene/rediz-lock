local numKeyArgs = 2
local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock then
	return { 0, existingWriteLock }
else
	redis.call("incr", KEYS[2])
	if ARGV[1] ~= 0 and ARGV[1] ~= "0" then
		redis.call("expire", KEYS[2], ARGV[1])
	end
	return { 1 }
end
