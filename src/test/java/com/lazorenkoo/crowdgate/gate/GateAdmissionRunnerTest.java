package com.lazorenkoo.crowdgate.gate;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

class GateAdmissionRunnerTest {

    @Test
    void continuesAfterFailedGateAndRetriesItOnNextRound() {
        GateRepository repository = mock(GateRepository.class);
        GateAdmissionService admission = mock(GateAdmissionService.class);
        Gate first = mock(Gate.class);
        Gate next = mock(Gate.class);
        UUID firstId = UUID.randomUUID();
        UUID nextId = UUID.randomUUID();
        when(first.getId()).thenReturn(firstId);
        when(next.getId()).thenReturn(nextId);
        var page = PageRequest.of(0, 100);
        when(repository.findAdmissionGates(any(), isNull(), eq(page)))
                .thenReturn(List.of(first, next));
        when(repository.findAdmissionGates(any(), eq(firstId), eq(page)))
                .thenReturn(List.of(next));
        doThrow(new IllegalStateException("Unavailable")).doNothing().when(admission).admit(firstId);
        GateAdmissionRunner runner = new GateAdmissionRunner(repository, admission);

        runner.run();
        verify(admission).admit(firstId);
        verify(admission, never()).admit(nextId);
        runner.run();
        verify(repository).findAdmissionGates(any(), eq(firstId), eq(page));
        verify(admission).admit(nextId);
        runner.run();
        verify(repository, times(2)).findAdmissionGates(any(), isNull(), eq(page));
        verify(admission, times(2)).admit(firstId);
        verify(admission, times(2)).admit(nextId);
    }
}
