package com.lazorenkoo.crowdgatedemo;

import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.mock.web.server.MockWebSession;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DemoActivationFailureTest {

    private final DemoAdmissionVerifier verifier = mock(DemoAdmissionVerifier.class);
    private final DemoActivationClient activation = mock(DemoActivationClient.class);
    private final DemoController controller = new DemoController(verifier, activation);
    private final UUID owner = UUID.randomUUID();
    private final AdmissionClaims claims = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
            Instant.now().minusSeconds(1), Instant.now().plusSeconds(60), UUID.randomUUID());

    @Test
    void expiryDuringActivationDoesNotSaveAuthentication() {
        var expired = new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Expired admission");
        when(verifier.verify("Bearer credential")).thenReturn(claims).thenThrow(expired);
        Sinks.Empty<Void> response = Sinks.empty();
        when(activation.activate(claims, "credential", owner)).thenReturn(response.asMono());
        MockWebSession session = new MockWebSession();
        AtomicReference<Throwable> error = new AtomicReference<>();
        controller.enter(exchange(owner.toString()), session).subscribe(ignored -> fail("Unexpected access"), error::set);
        assertThat(session.isStarted()).isFalse();
        assertThat(response.tryEmitEmpty()).isEqualTo(Sinks.EmitResult.OK);
        assertThat(error.get()).isSameAs(expired);
        assertThat(session.getAttributes()).isEmpty();
        assertThat(session.isStarted()).isFalse();
        verify(verifier, times(2)).verify("Bearer credential");
        verify(activation).activate(claims, "credential", owner);
    }

    @Test
    void disconnectCancelsActivationWithoutStartingTheSession() {
        when(verifier.verify("Bearer credential")).thenReturn(claims);
        AtomicBoolean cancelled = new AtomicBoolean();
        when(activation.activate(claims, "credential", owner))
                .thenReturn(Mono.<Void>never().doOnCancel(() -> cancelled.set(true)));
        MockWebSession session = new MockWebSession();
        var subscription = controller.enter(exchange(owner.toString()), session).subscribe();
        subscription.dispose();
        assertThat(cancelled).isTrue();
        assertThat(session.getAttributes()).isEmpty();
        assertThat(session.isStarted()).isFalse();
        verify(verifier).verify("Bearer credential");
    }

    @Test
    void invalidIdentityIsRejectedBeforeCallingCrowdGate() {
        when(verifier.verify("Bearer credential")).thenReturn(claims);
        for (String invalid : new String[]{"invalid", "1-1-1-1-1", ""}) {
            MockWebSession session = new MockWebSession();
            assertThatThrownBy(() -> controller.enter(exchange(invalid), session).block())
                    .isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
            assertThat(session.getAttributes()).isEmpty();
            assertThat(session.isStarted()).isFalse();
        }
        verifyNoInteractions(activation);
    }

    private MockServerWebExchange exchange(String user) {
        return MockServerWebExchange.from(MockServerHttpRequest.post("/api/demo/enter")
                .header("Authorization", "Bearer credential").cookie(new HttpCookie("cg_user_id", user)));
    }
}
