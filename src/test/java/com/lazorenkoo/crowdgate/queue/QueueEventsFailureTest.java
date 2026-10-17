package com.lazorenkoo.crowdgate.queue;

import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class QueueEventsFailureTest {

    private final RedisQueue queue = mock(RedisQueue.class);
    private final UserIdentity identity = mock(UserIdentity.class);
    private final AdmissionTokenCodec tokens = mock(AdmissionTokenCodec.class);
    private final QueueEventsController controller = new QueueEventsController(queue, identity, tokens);
    private final UUID owner = UUID.randomUUID();
    private final String token = UUID.randomUUID() + ":" + UUID.randomUUID();
    private final QueueEntry waiting = new QueueEntry(token, "WAITING", 1, 1);
    private final MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/events"));

    @BeforeEach
    void identifyOwner() {
        when(identity.resolve(exchange)).thenReturn(owner);
    }

    @Test
    void slowRedisReadsDoNotOverlapAndDisconnectCancelsThePendingRead() {
        AtomicBoolean cancelled = new AtomicBoolean();
        when(queue.status(token, owner)).thenReturn(Mono.just(waiting),
                Mono.<QueueEntry>never().doOnCancel(() -> cancelled.set(true)));
        StepVerifier.withVirtualTime(() -> controller.events(token, exchange).block().getBody())
                .expectNextMatches(event -> event.data().entry().equals(waiting))
                .thenAwait(Duration.ofSeconds(30))
                .then(() -> verify(queue, times(2)).status(token, owner))
                .thenCancel().verify(Duration.ofSeconds(3));
        assertThat(cancelled).isTrue();
        verifyNoInteractions(tokens);
    }

    @Test
    void redisFailureTerminatesTheStreamWithoutContinuingToPoll() {
        var failure = new IllegalStateException("Redis unavailable");
        when(queue.status(token, owner)).thenReturn(Mono.just(waiting), Mono.error(failure));
        StepVerifier.withVirtualTime(() -> controller.events(token, exchange).block().getBody())
                .expectNextMatches(event -> event.data().entry().equals(waiting))
                .thenAwait(Duration.ofSeconds(10))
                .expectErrorMatches(error -> error == failure).verify(Duration.ofSeconds(3));
        verify(queue, times(2)).status(token, owner);
        verifyNoInteractions(tokens);
    }

    @Test
    void waitingStreamClosesAfterItsMaximumLifetime() {
        when(queue.status(token, owner)).thenReturn(Mono.just(waiting));
        StepVerifier.withVirtualTime(() -> controller.events(token, exchange).block().getBody())
                .expectNextMatches(event -> event.data().entry().equals(waiting))
                .thenAwait(Duration.ofMinutes(5))
                .thenConsumeWhile(event -> event.data().entry().equals(waiting))
                .expectComplete().verify(Duration.ofSeconds(3));
        verify(queue, atMost(301)).status(token, owner);
        verifyNoInteractions(tokens);
    }
}
