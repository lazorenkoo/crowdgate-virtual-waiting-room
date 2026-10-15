redis.call('SET', KEYS[6], '1')
redis.call('UNLINK', KEYS[1])
return 1
