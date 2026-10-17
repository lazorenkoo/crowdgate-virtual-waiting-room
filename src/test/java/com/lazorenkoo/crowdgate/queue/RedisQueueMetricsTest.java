package com.lazorenkoo.crowdgate.queue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisQueueMetricsTest {

    private final ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);

    @Test
    void measuresEachSubscriptionAndKeepsRedisFailuresSeparateFromBusinessRejections() {
        var registry = new SimpleMeterRegistry();
        try {
            var queue = new RedisQueue(redis, registry);
            var request = queue.join(UUID.randomUUID(), UUID.randomUUID());
            verifyNoInteractions(redis);
            assertThat(registry.find("crowdgate.redis.operation").timers()).isEmpty();
            var failure = new IllegalStateException("Redis unavailable");
            when(redis.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(
                    Flux.just(List.of("token", "WAITING", "1", "1")),
                    Flux.just(List.of("token", "WAITING", "1", "1")),
                    Flux.error(failure), Flux.just(List.of()));
            request.block(Duration.ofSeconds(2));
            request.block(Duration.ofSeconds(2));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(registry.get("crowdgate.redis.operation").tag("operation", "join")
                            .timer().count()).isEqualTo(2));
            assertThat(registry.find("crowdgate.redis.errors").counter()).isNull();
            assertThatThrownBy(() -> request.block(Duration.ofSeconds(2))).isSameAs(failure);
            assertThatThrownBy(() -> request.block(Duration.ofSeconds(2)))
                    .isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode().value()).isEqualTo(410));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(registry.get("crowdgate.redis.operation").tag("operation", "join")
                            .timer().count()).isEqualTo(4));
            assertThat(registry.get("crowdgate.redis.errors").tag("operation", "join").counter().count())
                    .isEqualTo(1);
            verify(redis, times(4)).execute(any(RedisScript.class), anyList(), anyList());
        } finally {
            registry.close();
        }
    }

    @Test
    void cancellationRecordsElapsedTimeWithoutCountingAnError() {
        var registry = new SimpleMeterRegistry();
        try {
            var queue = new RedisQueue(redis, registry);
            when(redis.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(Flux.never());
            var subscription = queue.admit(UUID.randomUUID(), 1, 1, 60).subscribe();
            subscription.dispose();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(registry.get("crowdgate.redis.operation").tag("operation", "admit")
                            .timer().count()).isEqualTo(1));
            assertThat(registry.find("crowdgate.redis.errors").counter()).isNull();
        } finally {
            registry.close();
        }
    }
}
