local token = ARGV[1]
if redis.call('HGET', KEYS[4], token) ~= ARGV[2] then
    return {'404'}
end
if redis.call('EXISTS', KEYS[6]) == 1 then
    return {'410'}
end
local expires = redis.call('ZSCORE', KEYS[7], token)
local issued = redis.call('HGET', KEYS[12], token)
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[3], token) ~= 'ADMITTED' or not issued
        or not expires or tonumber(expires) <= now then
    return {'409'}
end
redis.call('HSETNX', KEYS[13], token, ARGV[3])
return {issued, expires, redis.call('HGET', KEYS[13], token)}
