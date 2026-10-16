if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then
    return {}
end
local status = redis.call('HGET', KEYS[3], ARGV[1])
if not status then
    return {}
end
if redis.call('EXISTS', KEYS[6]) == 1 and (status == 'WAITING' or status == 'ADMITTED') then
    status = 'CANCELLED'
    redis.call('HSET', KEYS[3], ARGV[1], status)
end
if status == 'ADMITTED' or status == 'ACTIVE' then
    local expiryKey = status == 'ACTIVE' and KEYS[9] or KEYS[7]
    local expires = redis.call('ZSCORE', expiryKey, ARGV[1])
    local time = redis.call('TIME')
    local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    if expires and tonumber(expires) <= now then
        status = 'EXPIRED'
        redis.call('HSET', KEYS[3], ARGV[1], status)
        redis.call('ZREM', expiryKey, ARGV[1])
        local credential = redis.call('HGET', KEYS[10], ARGV[1])
        if credential then
            redis.call('HDEL', KEYS[11], credential)
            redis.call('HDEL', KEYS[10], ARGV[1])
        end
    end
end
local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
return {ARGV[1], status, tostring(rank and rank + 1 or 0), tostring(redis.call('ZCARD', KEYS[1]))}
