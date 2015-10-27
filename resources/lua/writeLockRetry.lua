local numKeyArgs = 2
if redis.call("get", KEYS[1]) == ARGV[1] then
	if ARGV[2] ~= 0 and ARGV[2] ~= "0" then
		redis.call("expire", KEYS[1], ARGV[2])
	end
	if redis.call("exists", KEYS[2]) == 1 then
		return { 2 }
	else
		return { 1 }
	end
else
	return { 0 }
end
