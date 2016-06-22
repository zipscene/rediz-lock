local numKeyArgs = 2
local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock == ARGV[1] or not existingWriteLock then
	if not existingWriteLock then
		redis.call("set", KEYS[1], ARGV[1])
	end
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("expire", KEYS[1], ARGV[2])
	end
	if redis.call("exists", KEYS[2]) == 1 then
		return { 2 }
	else
		return { 1 }
	end
else
	return { 0, existingWriteLock }
end
