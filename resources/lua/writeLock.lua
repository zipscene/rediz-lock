local numKeyArgs = 2
if redis.call("exists", KEYS[1]) == 1 then
	return 0
else
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[2])
	else
		redis.call("set", KEYS[1], ARGV[1])
	end
	if redis.call("exists", KEYS[2]) == 1 then
		return 2
	else
		return 1
	end
end
