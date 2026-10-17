package com.lazorenkoo.crowdgate.metrics;

import com.lazorenkoo.crowdgate.gate.GateRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class QueueMetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(QueueMetricsCollector.class);
    private final GateRepository repository;
    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<List> script;
    private final Counter errors;
    private UUID cursor;
    private long waiting;
    private long reserved;
    private long active;
    private volatile Snapshot snapshot;

    public QueueMetricsCollector(GateRepository repository, ReactiveStringRedisTemplate redis,
                                 MeterRegistry registry) {
        this.repository = repository;
        this.redis = redis;
        script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/queue-metrics.lua"));
        script.setResultType(List.class);
        errors = registry.counter("crowdgate.metrics.collection.errors");
        Gauge.builder("crowdgate.queue.size", this, value -> value.waiting()).register(registry);
        Gauge.builder("crowdgate.reserved.slots", this, value -> value.reserved()).register(registry);
        Gauge.builder("crowdgate.active.sessions", this, value -> value.active()).register(registry);
        Gauge.builder("crowdgate.metrics.snapshot.age", this, value -> value.age())
                .baseUnit("seconds").register(registry);
    }

    @Scheduled(scheduler = "metricsScheduler", fixedDelayString = "${crowdgate.metrics.delay-ms:5000}")
    public synchronized void collect() {
        try {
            var gates = repository.findMetricGateIds(cursor, PageRequest.of(0, 25));
            List<Counts> batch = Flux.fromIterable(gates).concatMap(this::read, 1)
                    .collectList().block(Duration.ofSeconds(2));
            for (Counts counts : batch) {
                waiting += counts.waiting();
                reserved += counts.reserved();
                active += counts.active();
            }
            if (gates.size() < 25) {
                snapshot = new Snapshot(waiting, reserved, active, Instant.now());
                reset();
            } else {
                cursor = gates.getLast();
            }
        } catch (RuntimeException error) {
            reset();
            errors.increment();
            log.warn("Queue metrics collection failed; keeping the previous snapshot");
        }
    }

    private reactor.core.publisher.Mono<Counts> read(UUID gate) {
        String prefix = "cg:{" + gate + "}:";
        return redis.execute(script, List.of(prefix + "waiting", prefix + "reservations", prefix + "active"))
                .single().map(values -> new Counts(number(values.get(0)), number(values.get(1)),
                        number(values.get(2))));
    }

    private long number(Object value) {
        return Long.parseLong(value.toString());
    }

    private void reset() {
        cursor = null;
        waiting = 0;
        reserved = 0;
        active = 0;
    }

    private double waiting() {
        Snapshot current = snapshot;
        return current == null ? Double.NaN : current.waiting();
    }

    private double reserved() {
        Snapshot current = snapshot;
        return current == null ? Double.NaN : current.reserved();
    }

    private double active() {
        Snapshot current = snapshot;
        return current == null ? Double.NaN : current.active();
    }

    private double age() {
        Snapshot current = snapshot;
        return current == null ? Double.NaN
                : Math.max(0, Duration.between(current.completedAt(), Instant.now()).toMillis() / 1000.0);
    }

    private record Counts(long waiting, long reserved, long active) {
    }

    private record Snapshot(long waiting, long reserved, long active, Instant completedAt) {
    }
}
