if redis.call('EXISTS', KEYS[6]) == 1 then
    return {}
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expiredActive = redis.call('ZRANGEBYSCORE', KEYS[9], '-inf', now, 'LIMIT', 0, 100)
for _, token in ipairs(expiredActive) do
    local credential = redis.call('HGET', KEYS[10], token)
    if credential then
        redis.call('HDEL', KEYS[11], credential)
        redis.call('HDEL', KEYS[10], token)
    end
    if redis.call('HGET', KEYS[3], token) == 'ACTIVE' then
        redis.call('HSET', KEYS[3], token, 'EXPIRED')
    end
    redis.call('ZREM', KEYS[9], token)
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[7], '-inf', now, 'LIMIT', 0, 100)
for _, token in ipairs(expired) do
    local status = redis.call('HGET', KEYS[3], token)
    if status == 'ADMITTED' then
        redis.call('HSET', KEYS[3], token, 'EXPIRED')
    end
    if status ~= 'ACTIVE' then
        redis.call('ZREM', KEYS[7], token)
    end
end
local second = time[1]
if redis.call('HGET', KEYS[8], 'second') ~= second then
    redis.call('HSET', KEYS[8], 'second', second, 'count', 0)
end
local used = tonumber(redis.call('HGET', KEYS[8], 'count'))
local capacity = tonumber(ARGV[2]) - redis.call('ZCARD', KEYS[7]) - redis.call('ZCARD', KEYS[9])
local count = math.min(tonumber(ARGV[1]) - used, capacity, 100)
local admitted = {}
for i = 1, count do
    local first = redis.call('ZPOPMIN', KEYS[1], 1)
    if #first == 0 then
        break
    end
    local token = first[1]
    redis.call('HSET', KEYS[3], token, 'ADMITTED')
    redis.call('HSET', KEYS[12], token, now)
    redis.call('ZADD', KEYS[7], now + tonumber(ARGV[3]) * 1000, token)
    table.insert(admitted, token)
end
if #admitted > 0 then
    redis.call('HINCRBY', KEYS[8], 'count', #admitted)
end
return admitted
