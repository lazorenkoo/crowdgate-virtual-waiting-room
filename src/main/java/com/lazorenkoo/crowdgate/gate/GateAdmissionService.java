package com.lazorenkoo.crowdgate.gate;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class GateAdmissionService {

    private final GateRepository repository;
    private final RedisQueue queue;

    public GateAdmissionService(GateRepository repository, RedisQueue queue) {
        this.repository = repository;
        this.queue = queue;
    }

    @Transactional(timeout = 3)
    public void admit(UUID id) {
        Gate gate = repository.findForAdmission(id).orElse(null);
        if (gate == null || gate.getStatus() != GateStatus.OPEN) {
            return;
        }
        if (gate.getClosesAt() != null && !gate.getClosesAt().isAfter(Instant.now())) {
            return;
        }
        queue.admit(id, gate.getAdmissionRate(), gate.getMaxActiveUsers(), gate.getTokenTtlSeconds())
                .block(Duration.ofSeconds(2));
    }
}
