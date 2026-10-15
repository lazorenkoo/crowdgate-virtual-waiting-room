if redis.call('EXISTS', KEYS[6]) == 1 then
    return {}
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local capacity = tonumber(ARGV[2]) - redis.call('ZCARD', KEYS[7])
local count = math.min(tonumber(ARGV[1]), capacity, 100)
local admitted = {}
for i = 1, count do
    local first = redis.call('ZPOPMIN', KEYS[1], 1)
    if #first == 0 then
        break
    end
    local token = first[1]
    redis.call('HSET', KEYS[3], token, 'ADMITTED')
    redis.call('ZADD', KEYS[7], now + tonumber(ARGV[3]) * 1000, token)
    table.insert(admitted, token)
end
return admitted
