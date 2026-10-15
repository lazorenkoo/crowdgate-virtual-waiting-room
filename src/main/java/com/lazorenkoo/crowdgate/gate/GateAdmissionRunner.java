package com.lazorenkoo.crowdgate.gate;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class GateAdmissionRunner {

    private static final Logger log = LoggerFactory.getLogger(GateAdmissionRunner.class);
    private final GateRepository repository;
    private final RedisQueue queue;
    private UUID cursor;

    public GateAdmissionRunner(GateRepository repository, RedisQueue queue) {
        this.repository = repository;
        this.queue = queue;
    }

    @Scheduled(fixedDelayString = "${crowdgate.admission.delay-ms:1000}")
    public void run() {
        var gates = repository.findAdmissionGates(Instant.now(), cursor, PageRequest.of(0, 100));
        for (Gate gate : gates) {
            try {
                if (gate.getClosesAt() == null || gate.getClosesAt().isAfter(Instant.now())) {
                    queue.admit(gate.getId(), gate.getAdmissionRate(), gate.getMaxActiveUsers(),
                            gate.getTokenTtlSeconds()).block(Duration.ofSeconds(2));
                }
                cursor = gate.getId();
            } catch (RuntimeException error) {
                log.warn("Admission will be retried for gate {}", gate.getId());
                return;
            }
        }
        if (gates.size() < 100) {
            cursor = null;
        }
    }
}
