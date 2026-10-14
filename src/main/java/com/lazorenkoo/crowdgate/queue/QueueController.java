package com.lazorenkoo.crowdgate.queue;

import com.lazorenkoo.crowdgate.gate.GateService;
import com.lazorenkoo.crowdgate.gate.GateStatus;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class QueueController {

    private final GateService gates;
    private final RedisQueue queue;
    private final UserIdentity identity;

    public QueueController(GateService gates, RedisQueue queue, UserIdentity identity) {
        this.gates = gates;
        this.queue = queue;
        this.identity = identity;
    }

    @PostMapping("/gates/{gateId}/join")
    public Mono<QueueEntry> join(@PathVariable UUID gateId, ServerWebExchange exchange) {
        return gates.findById(gateId).flatMap(gate -> {
            if (gate.getStatus() == GateStatus.CLOSED || gate.getClosesAt() != null
                    && !gate.getClosesAt().isAfter(Instant.now())) {
                return Mono.error(new ResponseStatusException(HttpStatus.GONE, "Gate is closed"));
            }
            if (gate.getStatus() != GateStatus.OPEN && gate.getStatus() != GateStatus.PAUSED) {
                return Mono.error(new ResponseStatusException(HttpStatus.CONFLICT, "Gate is not open"));
            }
            return queue.join(gateId, identity.resolve(exchange));
        });
    }

    @GetMapping("/queue/{queueToken}")
    public Mono<QueueEntry> status(@PathVariable String queueToken, ServerWebExchange exchange) {
        return queue.status(queueToken, identity.resolve(exchange));
    }
}
