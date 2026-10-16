if redis.call('EXISTS', KEYS[6]) == 1 then
    return {}
end
local token = redis.call('HGET', KEYS[2], ARGV[1])
local status = token and redis.call('HGET', KEYS[3], token)
if status == 'ADMITTED' or status == 'ACTIVE' then
    local expiryKey = status == 'ACTIVE' and KEYS[9] or KEYS[7]
    local expires = redis.call('ZSCORE', expiryKey, token)
    local time = redis.call('TIME')
    local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    if expires and tonumber(expires) <= now then
        status = 'EXPIRED'
        redis.call('HSET', KEYS[3], token, status)
        redis.call('ZREM', expiryKey, token)
        local credential = redis.call('HGET', KEYS[10], token)
        if credential then
            redis.call('HDEL', KEYS[11], credential)
            redis.call('HDEL', KEYS[10], token)
        end
    end
end
if not status or (status ~= 'WAITING' and status ~= 'ADMITTED' and status ~= 'ACTIVE') then
    token = ARGV[2]
    status = 'WAITING'
    local sequence = redis.call('INCR', KEYS[5])
    redis.call('ZADD', KEYS[1], sequence, token)
    redis.call('HSET', KEYS[2], ARGV[1], token)
    redis.call('HSET', KEYS[3], token, status)
    redis.call('HSET', KEYS[4], token, ARGV[1])
end
local rank = redis.call('ZRANK', KEYS[1], token)
return {token, status, tostring(rank and rank + 1 or 0), tostring(redis.call('ZCARD', KEYS[1]))}
