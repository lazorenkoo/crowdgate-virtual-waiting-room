package com.lazorenkoo.crowdgate.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulingConfigurationTest {

    @Test
    void blockedMetricWorkerDoesNotBlockGateScheduling() throws Exception {
        var context = new AnnotationConfigApplicationContext(SchedulingConfiguration.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            var metrics = context.getBean("metricsScheduler", ThreadPoolTaskScheduler.class);
            var gates = context.getBean("taskScheduler", ThreadPoolTaskScheduler.class);
            assertThat(metrics).isNotSameAs(gates);
            var blocked = metrics.submit(() -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Metric worker was not released");
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(error);
                }
                return Thread.currentThread().getName();
            });
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            var independent = gates.submit(() -> Thread.currentThread().getName());
            assertThat(independent.get(2, TimeUnit.SECONDS)).startsWith("cg-schedule-");
            assertThat(blocked.isDone()).isFalse();
            release.countDown();
            assertThat(blocked.get(2, TimeUnit.SECONDS)).startsWith("cg-metrics-");
        } finally {
            release.countDown();
            context.close();
        }
    }

    @Test
    void disabledSchedulingDoesNotCreateWorkerPools() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test",
                    Map.of("crowdgate.schedule.enabled", "false")));
            context.register(SchedulingConfiguration.class);
            context.refresh();
            assertThat(context.getBeansOfType(ThreadPoolTaskScheduler.class)).isEmpty();
        }
    }
}
