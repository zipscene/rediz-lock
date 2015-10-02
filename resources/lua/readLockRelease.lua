local numKeyArgs = 1
if redis.call("exists", KEYS[1]) == 1 then
	if redis.call("decr", KEYS[1]) <= 0 then
		redis.call("del", KEYS[1])
	end
	return 1
else
	return 1
end
