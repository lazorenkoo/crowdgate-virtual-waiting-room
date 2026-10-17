package com.lazorenkoo.crowdgate.queue;

import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@RestController
public class QueueEventsController {

    private final RedisQueue queue;
    private final UserIdentity identity;
    private final AdmissionTokenCodec tokens;

    public QueueEventsController(RedisQueue queue, UserIdentity identity, AdmissionTokenCodec tokens) {
        this.queue = queue;
        this.identity = identity;
        this.tokens = tokens;
    }

    @GetMapping(value = "/api/v1/queue/{queueToken}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<ServerSentEvent<QueueEvent>>>> events(
            @PathVariable String queueToken, ServerWebExchange exchange) {
        UUID owner = identity.resolve(exchange);
        return queue.status(queueToken, owner).flatMap(first -> event(first, owner).map(initial -> {
            Flux<QueueEntry> updates = Flux.interval(Duration.ofSeconds(1)).onBackpressureDrop()
                    .concatMap(tick -> queue.status(queueToken, owner), 1);
            Flux<ServerSentEvent<QueueEvent>> stream = Flux.concat(Mono.just(initial),
                            updates.concatMap(entry -> event(entry, owner), 1))
                    .takeUntil(value -> !"WAITING".equals(value.entry().status()))
                    .take(Duration.ofMinutes(5))
                    .map(value -> ServerSentEvent.builder(value).event("queue").build());
            return ResponseEntity.ok().header("Cache-Control", "no-store")
                    .header("X-Accel-Buffering", "no").body(stream);
        }));
    }

    private Mono<QueueEvent> event(QueueEntry entry, UUID owner) {
        if (!"ADMITTED".equals(entry.status())) {
            return Mono.just(new QueueEvent(entry, null));
        }
        return queue.admissionClaims(entry.queueToken(), owner)
                .map(claims -> new QueueEvent(entry, tokens.sign(claims)));
    }

    public record QueueEvent(QueueEntry entry, String admissionToken) {
    }
}
