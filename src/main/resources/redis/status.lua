if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then
    return {}
end
local status = redis.call('HGET', KEYS[3], ARGV[1])
if not status then
    return {}
end
local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
return {ARGV[1], status, tostring(rank and rank + 1 or 0), tostring(redis.call('ZCARD', KEYS[1]))}
