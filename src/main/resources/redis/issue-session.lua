local entry = ARGV[1]
if redis.call('HGET', KEYS[4], entry) ~= ARGV[2] then
    return {}
end
local expires = redis.call('ZSCORE', KEYS[9], entry)
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[3], entry) ~= 'ACTIVE' or not expires or tonumber(expires) <= now then
    return {}
end
local secret = redis.call('HGET', KEYS[10], entry)
if not secret then
    secret = ARGV[3]
    redis.call('HSET', KEYS[10], entry, secret)
    redis.call('HSET', KEYS[11], secret, entry)
end
return {secret, expires}
