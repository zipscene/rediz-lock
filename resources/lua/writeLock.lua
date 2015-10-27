local numKeyArgs = 2
local existingWriteLock = redis.call("get", KEYS[1])
if existingWriteLock then
	return { 0, existingWriteLock }
else
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[2])
	else
		redis.call("set", KEYS[1], ARGV[1])
	end
	if redis.call("exists", KEYS[2]) == 1 then
		return { 2 }
	else
		return { 1 }
	end
end
