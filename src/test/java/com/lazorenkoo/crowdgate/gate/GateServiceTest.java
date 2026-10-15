package com.lazorenkoo.crowdgate.gate;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GateServiceTest {

    private final GateRepository repository = mock(GateRepository.class);
    private final RedisQueue queue = mock(RedisQueue.class);
    private final GateService service = new GateService(repository, queue);

    @Test
    void retriesRedisCloseAfterGateHasAlreadyClosed() {
        UUID id = UUID.randomUUID();
        Gate gate = new Gate("Retry close", 10, 100, 300, 120);
        gate.changeStatus(GateStatus.OPEN);
        when(repository.findById(id)).thenReturn(Optional.of(gate));
        when(repository.saveAndFlush(gate)).thenReturn(gate);
        when(queue.close(id)).thenReturn(Mono.error(new RedisConnectionFailureException("Unavailable")))
                .thenReturn(Mono.empty());

        assertThatThrownBy(() -> service.changeStatus(id, GateStatus.CLOSED).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(503));
        assertThat(gate.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(service.changeStatus(id, GateStatus.CLOSED).block(Duration.ofSeconds(5)))
                .isSameAs(gate);
        var order = inOrder(repository, queue);
        order.verify(repository).findById(id);
        order.verify(repository).saveAndFlush(gate);
        order.verify(queue).close(id);
        verify(queue, times(2)).close(id);
    }

    @Test
    void createsGateLazilyOutsideCallingThread() {
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> databaseThread = new AtomicReference<>();
        when(repository.saveAndFlush(any(Gate.class))).thenAnswer(invocation -> {
            databaseThread.set(Thread.currentThread());
            return invocation.getArgument(0);
        });
        Mono<Gate> result = service.create(new CreateGateRequest(" Launch ", 10, 100, 300, 120));

        verifyNoInteractions(repository);
        Gate gate = result.block(Duration.ofSeconds(5));

        assertThat(databaseThread.get()).isNotNull().isNotSameAs(caller);
        assertThat(gate.getName()).isEqualTo("Launch");
        assertThat(gate.getAdmissionRate()).isEqualTo(10);
        assertThat(gate.getMaxActiveUsers()).isEqualTo(100);
        assertThat(gate.getSessionTtlSeconds()).isEqualTo(300);
        assertThat(gate.getTokenTtlSeconds()).isEqualTo(120);
    }

    @Test
    void rejectsInvalidSettingsBeforeSaving() {
        CreateGateRequest[] invalid = {
                new CreateGateRequest(" ", 10, 100, 300, 120),
                new CreateGateRequest("Launch", 0, 100, 300, 120),
                new CreateGateRequest("Launch", 10, 100, 0, 120)
        };
        for (CreateGateRequest request : invalid) {
            assertThatThrownBy(() -> service.create(request).block(Duration.ofSeconds(5)))
                    .isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode().value()).isEqualTo(400));
        }
        verifyNoInteractions(repository);
    }

    @Test
    void reportsMissingGate() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.findById(id).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
    }
}
