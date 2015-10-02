local numKeyArgs = 2
if redis.call("exists", KEYS[1]) == 1 then
	return 0
else
	redis.call("incr", KEYS[2])
	if ARGV[1] ~= 0 and ARGV[1] ~= "0" then
		redis.call("expire", KEYS[2], ARGV[1])
	end
	return 1
end
