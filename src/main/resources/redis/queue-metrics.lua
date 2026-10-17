local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
return {redis.call('ZCARD', KEYS[1]),
        redis.call('ZCOUNT', KEYS[2], '(' .. now, '+inf'),
        redis.call('ZCOUNT', KEYS[3], '(' .. now, '+inf')}
