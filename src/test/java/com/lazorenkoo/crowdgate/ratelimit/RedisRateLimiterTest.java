package com.lazorenkoo.crowdgate.ratelimit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RedisRateLimiterTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7.4").withExposedPorts(6379);
    static LettuceConnectionFactory connection;
    static ReactiveStringRedisTemplate template;

    @BeforeAll
    static void connect() {
        connection = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connection.afterPropertiesSet();
        connection.start();
        template = new ReactiveStringRedisTemplate(connection);
    }

    @AfterAll
    static void disconnect() {
        connection.destroy();
    }

    @Test
    void sharesOneBucketAcrossConcurrentInstances() {
        UUID gate = UUID.randomUUID();
        UUID client = UUID.randomUUID();
        var first = new RedisRateLimiter(template);
        var second = new RedisRateLimiter(template);
        var results = Flux.range(0, 100).flatMap(index -> (index % 2 == 0 ? first : second)
                .acquire(gate, client, 3, 1), 16).collectList().block(Duration.ofSeconds(5));
        assertThat(results).filteredOn(delay -> delay == 0).hasSize(3);
        assertThat(results).filteredOn(delay -> delay > 0).hasSize(97);
        assertThat(first.acquire(gate, UUID.randomUUID(), 3, 1).block()).isZero();
        assertThat(first.acquire(UUID.randomUUID(), client, 3, 1).block()).isZero();
        Duration ttl = template.getExpire("cg:{" + gate + "}:rate-limit:" + client).block();
        assertThat(ttl).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(3));
    }

    @Test
    void refillsFromRedisTimeAndRejectsInvalidLimits() {
        UUID gate = UUID.randomUUID();
        UUID client = UUID.randomUUID();
        var limiter = new RedisRateLimiter(template);
        String key = "cg:{" + gate + "}:rate-limit:" + client;
        String seed = "local t=redis.call('TIME'); local n=t[1]*1000+math.floor(t[2]/1000); "
                + "redis.call('HSET',KEYS[1],'tokens',0,'updated',n-2000); return 1";
        template.execute(new DefaultRedisScript<>(seed, Long.class), List.of(key)).blockLast();
        assertThat(limiter.acquire(gate, client, 1, 1).block()).isZero();
        assertThat(limiter.acquire(gate, client, 1, 1).block()).isBetween(1L, 1000L);
        assertThatThrownBy(() -> limiter.acquire(gate, client, 0, 1).block())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.acquire(gate, client, 1, 0).block())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
