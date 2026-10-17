package com.lazorenkoo.crowdgate.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueueEventsControllerTest {

    private final RedisQueue queue = mock(RedisQueue.class);
    private final UserIdentity identity = mock(UserIdentity.class);
    private final AdmissionTokenCodec codec = new AdmissionTokenCodec(new ObjectMapper().findAndRegisterModules(),
            "queue-events-test-signing-key-with-at-least-32-bytes");
    private final QueueEventsController controller = new QueueEventsController(queue, identity, codec);
    private final UUID owner = UUID.randomUUID();
    private final String token = UUID.randomUUID() + ":" + UUID.randomUUID();
    private final MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/events"));

    @Test
    void streamsWaitingPositionThenSignedAdmissionAndCompletes() {
        when(identity.resolve(exchange)).thenReturn(owner);
        QueueEntry waiting = new QueueEntry(token, "WAITING", 3, 10);
        QueueEntry admitted = new QueueEntry(token, "ADMITTED", 0, 9);
        when(queue.status(token, owner)).thenReturn(Mono.just(waiting), Mono.just(admitted));
        AdmissionClaims claims = new AdmissionClaims(UUID.fromString(token.split(":")[0]),
                UUID.fromString(token.split(":")[1]), Instant.now().minusSeconds(1),
                Instant.now().plusSeconds(60), UUID.randomUUID());
        when(queue.admissionClaims(token, owner)).thenReturn(Mono.just(claims));
        var response = controller.events(token, exchange).block();
        assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
        var events = response.getBody().collectList().block(Duration.ofSeconds(5));
        assertThat(events).hasSize(2);
        assertThat(events.getFirst().event()).isEqualTo("queue");
        assertThat(events.getFirst().data().entry().position()).isEqualTo(3);
        assertThat(events.getFirst().data().admissionToken()).isNull();
        assertThat(codec.verify(events.getLast().data().admissionToken())).isEqualTo(claims);
        verify(queue, times(2)).status(token, owner);
    }

    @Test
    void rejectsForeignOwnersBeforeCreatingTheStream() {
        when(identity.resolve(exchange)).thenReturn(owner);
        when(queue.status(token, owner)).thenReturn(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND)));
        assertThatThrownBy(() -> controller.events(token, exchange).block())
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
        verify(queue, never()).admissionClaims(anyString(), any());
    }

    @Test
    void cancellationAndClosedEntriesDoNotStartPolling() {
        when(identity.resolve(exchange)).thenReturn(owner);
        when(queue.status(token, owner)).thenReturn(Mono.just(new QueueEntry(token, "WAITING", 1, 1)));
        var first = controller.events(token, exchange).block().getBody().take(1).collectList().block();
        assertThat(first).hasSize(1);
        verify(queue).status(token, owner);
        when(queue.status(token, owner)).thenReturn(Mono.just(new QueueEntry(token, "CANCELLED", 0, 0)));
        var closed = controller.events(token, exchange).block().getBody().collectList().block(Duration.ofSeconds(2));
        assertThat(closed).hasSize(1);
        assertThat(closed.getFirst().data().entry().status()).isEqualTo("CANCELLED");
        verify(queue, never()).admissionClaims(anyString(), any());
    }
}
