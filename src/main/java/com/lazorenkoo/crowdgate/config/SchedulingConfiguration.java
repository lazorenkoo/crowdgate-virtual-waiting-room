package com.lazorenkoo.crowdgate.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@ConditionalOnProperty(name = "crowdgate.schedule.enabled", havingValue = "true", matchIfMissing = true)
public class SchedulingConfiguration {

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        return scheduler("cg-schedule-");
    }

    @Bean
    public ThreadPoolTaskScheduler metricsScheduler() {
        return scheduler("cg-metrics-");
    }

    private ThreadPoolTaskScheduler scheduler(String prefix) {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(prefix);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
}
