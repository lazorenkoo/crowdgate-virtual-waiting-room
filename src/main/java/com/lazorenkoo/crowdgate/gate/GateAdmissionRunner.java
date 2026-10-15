package com.lazorenkoo.crowdgate.gate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class GateAdmissionRunner {

    private static final Logger log = LoggerFactory.getLogger(GateAdmissionRunner.class);
    private final GateRepository repository;
    private final GateAdmissionService admission;
    private UUID cursor;

    public GateAdmissionRunner(GateRepository repository, GateAdmissionService admission) {
        this.repository = repository;
        this.admission = admission;
    }

    @Scheduled(fixedDelayString = "${crowdgate.admission.delay-ms:1000}")
    public void run() {
        var gates = repository.findAdmissionGates(Instant.now(), cursor, PageRequest.of(0, 100));
        for (Gate gate : gates) {
            try {
                admission.admit(gate.getId());
                cursor = gate.getId();
            } catch (RuntimeException error) {
                log.warn("Admission will be retried for gate {}", gate.getId());
                cursor = gate.getId();
                return;
            }
        }
        if (gates.size() < 100) {
            cursor = null;
        }
    }
}
