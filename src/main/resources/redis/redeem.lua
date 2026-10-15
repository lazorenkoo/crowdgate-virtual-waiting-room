local token = ARGV[1]
if redis.call('HGET', KEYS[4], token) ~= ARGV[2] then
    return {'404'}
end
local status = redis.call('HGET', KEYS[3], token)
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if status == 'ACTIVE' then
    local expires = redis.call('ZSCORE', KEYS[9], token)
    if not expires or tonumber(expires) <= now then
        redis.call('HSET', KEYS[3], token, 'EXPIRED')
        redis.call('ZREM', KEYS[9], token)
        return {'409'}
    end
else
    if redis.call('EXISTS', KEYS[6]) == 1 then
        return {'410'}
    end
    if status ~= 'ADMITTED' then
        return {'409'}
    end
    local expires = redis.call('ZSCORE', KEYS[7], token)
    if not expires or tonumber(expires) <= now then
        redis.call('HSET', KEYS[3], token, 'EXPIRED')
        redis.call('ZREM', KEYS[7], token)
        return {'409'}
    end
    redis.call('ZREM', KEYS[7], token)
    redis.call('ZADD', KEYS[9], now + tonumber(ARGV[3]) * 1000, token)
    redis.call('HSET', KEYS[3], token, 'ACTIVE')
end
return {token, 'ACTIVE', '0', tostring(redis.call('ZCARD', KEYS[1]))}
