local entry = redis.call('HGET', KEYS[11], ARGV[1])
if not entry or redis.call('HGET', KEYS[4], entry) ~= ARGV[2] then
    return {}
end
local expires = redis.call('ZSCORE', KEYS[9], entry)
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[3], entry) ~= 'ACTIVE' or not expires or tonumber(expires) <= now then
    return {}
end
return {ARGV[1], expires}
