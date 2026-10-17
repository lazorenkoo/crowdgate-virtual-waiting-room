package com.lazorenkoo.crowdgate.metrics;

import com.lazorenkoo.crowdgate.gate.GateRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueueMetricsCollectorTest {

    private final GateRepository repository = mock(GateRepository.class);
    private final ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);

    @Test
    void publishesOnlyCompleteSweepsAndScrapesDoNotReadStorage() {
        var registry = new SimpleMeterRegistry();
        try {
            var collector = new QueueMetricsCollector(repository, redis, registry);
            List<UUID> page = IntStream.range(0, 25).mapToObj(index -> UUID.randomUUID()).toList();
            when(repository.findMetricGateIds(null, PageRequest.of(0, 25))).thenReturn(page);
            when(repository.findMetricGateIds(page.getLast(), PageRequest.of(0, 25)))
                    .thenReturn(List.of(UUID.randomUUID()));
            when(redis.execute(any(RedisScript.class), anyList()))
                    .thenReturn(Flux.just(List.of(2L, 3L, 4L, 0L, 0L)));
            collector.collect();
            assertThat(registry.get("crowdgate.queue.size").gauge().value()).isNaN();
            collector.collect();
            clearInvocations(redis, repository);
            assertThat(registry.get("crowdgate.queue.size").gauge().value()).isEqualTo(52);
            assertThat(registry.get("crowdgate.reserved.slots").gauge().value()).isEqualTo(78);
            assertThat(registry.get("crowdgate.active.sessions").gauge().value()).isEqualTo(104);
            assertThat(registry.get("crowdgate.metrics.snapshot.age").gauge().value()).isGreaterThanOrEqualTo(0);
            verifyNoInteractions(redis, repository);
        } finally {
            registry.close();
        }
    }

    @Test
    void keepsPreviousSnapshotOnFailureAndResetsTheNextSweep() {
        var registry = new SimpleMeterRegistry();
        try {
            var collector = new QueueMetricsCollector(repository, redis, registry);
            when(repository.findMetricGateIds(null, PageRequest.of(0, 25)))
                    .thenReturn(List.of(UUID.randomUUID()), List.of(UUID.randomUUID()), List.of());
            when(redis.execute(any(RedisScript.class), anyList()))
                    .thenReturn(Flux.just(List.of(7L, 2L, 1L, 0L, 0L)), Flux.error(new IllegalStateException()));
            collector.collect();
            collector.collect();
            assertThat(registry.get("crowdgate.queue.size").gauge().value()).isEqualTo(7);
            assertThat(registry.get("crowdgate.metrics.collection.errors").counter().count()).isEqualTo(1);
            collector.collect();
            assertThat(registry.get("crowdgate.queue.size").gauge().value()).isZero();
            assertThat(registry.get("crowdgate.reserved.slots").gauge().value()).isZero();
            assertThat(registry.get("crowdgate.active.sessions").gauge().value()).isZero();
            verify(repository, times(3)).findMetricGateIds(null, PageRequest.of(0, 25));
        } finally {
            registry.close();
        }
    }
}
