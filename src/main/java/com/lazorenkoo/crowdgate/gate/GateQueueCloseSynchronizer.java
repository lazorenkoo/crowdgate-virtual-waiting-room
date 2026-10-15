package com.lazorenkoo.crowdgate.gate;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.UUID;

@Component
public class GateQueueCloseSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(GateQueueCloseSynchronizer.class);
    private final GateRepository repository;
    private final RedisQueue queue;

    public GateQueueCloseSynchronizer(GateRepository repository, RedisQueue queue) {
        this.repository = repository;
        this.queue = queue;
    }

    public Mono<Void> close(UUID id) {
        return queue.close(id).then(Mono.fromRunnable(() -> repository.acknowledgeQueueClose(id))
                .subscribeOn(Schedulers.boundedElastic())).then();
    }

    @Scheduled(fixedDelayString = "${crowdgate.queue-close.delay-ms:1000}")
    public void run() {
        for (Gate gate : repository.findPendingQueueCloses(PageRequest.of(0, 100))) {
            try {
                close(gate.getId()).block(Duration.ofSeconds(5));
            } catch (RuntimeException error) {
                log.warn("Queue close will be retried for gate {}", gate.getId());
                break;
            }
        }
    }
}
