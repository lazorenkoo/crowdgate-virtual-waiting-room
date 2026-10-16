package com.lazorenkoo.crowdgate.ratelimit;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Component
public class RedisRateLimiter {

    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<Long> script;

    public RedisRateLimiter(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
        script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/rate-limit.lua"));
        script.setResultType(Long.class);
    }

    public Mono<Long> acquire(UUID gate, UUID client, int capacity, int refillPerSecond) {
        if (gate == null || client == null || capacity < 1 || refillPerSecond < 1) {
            return Mono.error(new IllegalArgumentException("Invalid rate limit"));
        }
        String key = "cg:{" + gate + "}:rate-limit:" + client;
        return redis.execute(script, List.of(key), List.of(Integer.toString(capacity),
                Integer.toString(refillPerSecond))).single();
    }
}
