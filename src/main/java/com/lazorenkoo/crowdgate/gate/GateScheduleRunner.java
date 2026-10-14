package com.lazorenkoo.crowdgate.gate;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "crowdgate.schedule.enabled", havingValue = "true", matchIfMissing = true)
public class GateScheduleRunner {

    private final GateRepository repository;

    public GateScheduleRunner(GateRepository repository) {
        this.repository = repository;
    }

    @Scheduled(fixedDelayString = "${crowdgate.schedule.delay-ms:1000}")
    public void run() {
        tick(Instant.now());
    }

    public void tick(Instant now) {
        repository.closeDue(now);
        repository.openDue(now);
    }
}
