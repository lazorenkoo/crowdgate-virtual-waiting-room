local token = ARGV[1]
if redis.call('HGET', KEYS[4], token) ~= ARGV[2] then
    return {'404'}
end
local status = redis.call('HGET', KEYS[3], token)
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if ARGV[4] and (redis.call('HGET', KEYS[13], token) ~= ARGV[4]
        or redis.call('HGET', KEYS[12], token) ~= ARGV[5]
        or redis.call('HGET', KEYS[14], token) ~= ARGV[6]
        or tonumber(ARGV[6]) <= now) then
    return {'401'}
end
if status == 'ACTIVE' then
    local expires = redis.call('ZSCORE', KEYS[9], token)
    if not expires or tonumber(expires) <= now then
        redis.call('HSET', KEYS[3], token, 'EXPIRED')
        redis.call('ZREM', KEYS[9], token)
        local credential = redis.call('HGET', KEYS[10], token)
        if credential then
            redis.call('HDEL', KEYS[11], credential)
            redis.call('HDEL', KEYS[10], token)
        end
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
