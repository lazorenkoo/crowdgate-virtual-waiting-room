package com.lazorenkoo.crowdgate.gate;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.RedisConnectionFailureException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;

class GateQueueCloseSynchronizerTest {

    private final GateRepository repository = mock(GateRepository.class);
    private final RedisQueue queue = mock(RedisQueue.class);
    private final GateQueueCloseSynchronizer synchronizer = new GateQueueCloseSynchronizer(repository, queue);

    @Test
    void retriesRedisFailureWithoutAcknowledgingOrContinuingBatch() {
        UUID id = UUID.randomUUID();
        Gate first = mock(Gate.class);
        Gate next = mock(Gate.class);
        when(first.getId()).thenReturn(id);
        when(repository.findPendingQueueCloses(PageRequest.of(0, 100)))
                .thenReturn(List.of(first, next)).thenReturn(List.of(first));
        when(queue.close(id)).thenReturn(Mono.error(new RedisConnectionFailureException("Unavailable")))
                .thenReturn(Mono.empty());

        synchronizer.run();
        verify(repository, never()).acknowledgeQueueClose(any());
        verifyNoInteractions(next);
        synchronizer.run();

        verify(queue, times(2)).close(id);
        verify(repository).acknowledgeQueueClose(id);
    }

    @Test
    void repeatsCloseWhenAcknowledgmentFailedAfterRedisSucceeded() {
        UUID id = UUID.randomUUID();
        Gate gate = mock(Gate.class);
        when(gate.getId()).thenReturn(id);
        when(repository.findPendingQueueCloses(PageRequest.of(0, 100))).thenReturn(List.of(gate));
        when(queue.close(id)).thenReturn(Mono.empty());
        when(repository.acknowledgeQueueClose(id))
                .thenThrow(new DataAccessResourceFailureException("Unavailable")).thenReturn(1);

        synchronizer.run();
        synchronizer.run();

        var order = inOrder(queue, repository);
        order.verify(repository).findPendingQueueCloses(PageRequest.of(0, 100));
        order.verify(queue).close(id);
        order.verify(repository).acknowledgeQueueClose(id);
        order.verify(repository).findPendingQueueCloses(PageRequest.of(0, 100));
        order.verify(queue).close(id);
        order.verify(repository).acknowledgeQueueClose(id);
    }

    @Test
    void skipsRedisWhenNoPendingClosesExist() {
        when(repository.findPendingQueueCloses(PageRequest.of(0, 100))).thenReturn(List.of());
        synchronizer.run();
        verifyNoInteractions(queue);
        verify(repository, never()).acknowledgeQueueClose(any());
    }
}
