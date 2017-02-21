local numKeyArgs = 1
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

