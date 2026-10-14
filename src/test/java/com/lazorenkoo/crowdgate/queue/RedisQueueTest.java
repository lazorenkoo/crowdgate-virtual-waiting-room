package com.lazorenkoo.crowdgate.queue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class RedisQueueTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7.4").withExposedPorts(6379);
    static LettuceConnectionFactory connection;
    static ReactiveStringRedisTemplate template;
    static RedisQueue queue;

    @BeforeAll
    static void connect() {
        connection = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connection.afterPropertiesSet();
        connection.start();
        template = new ReactiveStringRedisTemplate(connection);
        queue = new RedisQueue(template);
    }

    @AfterAll
    static void disconnect() {
        connection.destroy();
    }

    @Test
    void preservesFifoAndReusesIdentityWithinGate() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
        QueueEntry second = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        QueueEntry repeated = queue.join(gate, user).block(Duration.ofSeconds(5));
        assertThat(first.position()).isEqualTo(1);
        assertThat(second.position()).isEqualTo(2);
        assertThat(repeated.queueToken()).isEqualTo(first.queueToken());
        assertThat(repeated.queueSize()).isEqualTo(2);
        assertThat(repeated.position()).isEqualTo(1);
        assertThat(queue.join(UUID.randomUUID(), user).block(Duration.ofSeconds(5)).queueToken())
                .isNotEqualTo(first.queueToken());
    }

    @Test
    void reusesAdmittedAndActiveEntriesWithoutRejoiningWaitingQueue() {
        for (String state : new String[]{"ADMITTED", "ACTIVE"}) {
            UUID gate = UUID.randomUUID();
            UUID user = UUID.randomUUID();
            QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
            String prefix = "cg:{" + gate + "}:";
            template.opsForZSet().remove(prefix + "waiting", entry.queueToken()).block(Duration.ofSeconds(5));
            template.opsForHash().put(prefix + "statuses", entry.queueToken(), state).block(Duration.ofSeconds(5));
            QueueEntry repeated = queue.join(gate, user).block(Duration.ofSeconds(5));
            assertThat(repeated.queueToken()).isEqualTo(entry.queueToken());
            assertThat(repeated.status()).isEqualTo(state);
            assertThat(repeated.position()).isZero();
            assertThat(repeated.queueSize()).isZero();
        }
    }
}
