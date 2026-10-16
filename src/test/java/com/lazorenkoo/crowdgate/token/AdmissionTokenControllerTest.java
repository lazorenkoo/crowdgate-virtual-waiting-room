package com.lazorenkoo.crowdgate.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazorenkoo.crowdgate.queue.RedisQueue;
import com.lazorenkoo.crowdgate.queue.UserIdentity;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class AdmissionTokenControllerTest {

    @Test
    void validatesLocallyWithoutQueueOrIdentityAccess() {
        RedisQueue queue = mock(RedisQueue.class);
        UserIdentity identity = mock(UserIdentity.class);
        AdmissionTokenCodec codec = new AdmissionTokenCodec(new ObjectMapper().findAndRegisterModules(),
                "controller-test-signing-key-at-least-32-bytes");
        AdmissionTokenController controller = new AdmissionTokenController(queue, codec, identity);
        Instant now = Instant.now().minusSeconds(1);
        AdmissionClaims claims = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
                now, now.plusSeconds(60), UUID.randomUUID());
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.post("/api/v1/tokens/validate"));
        assertThat(controller.validate(new AdmissionTokenRequest(codec.sign(claims)), exchange)
                .block(Duration.ofSeconds(1))).isEqualTo(claims);
        assertThat(exchange.getResponse().getHeaders().getCacheControl()).isEqualTo("no-store");
        assertThatThrownBy(() -> controller.validate(new AdmissionTokenRequest("invalid"), exchange)
                .block(Duration.ofSeconds(1))).isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(401));
        verifyNoInteractions(queue, identity);
    }
}
