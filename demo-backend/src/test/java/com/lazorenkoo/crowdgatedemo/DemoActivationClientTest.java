package com.lazorenkoo.crowdgatedemo;

import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DemoActivationClientTest {

    private final UUID user = UUID.randomUUID();
    private final AdmissionClaims claims = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
            Instant.now(), Instant.now().plusSeconds(60), UUID.randomUUID());

    @Test
    void requiresMatchingActiveEntryAndForwardsOnlyIdentityCookie() {
        var builder = WebClient.builder().exchangeFunction(request -> {
            assertThat(request.method()).isEqualTo(HttpMethod.POST);
            assertThat(request.url().toString()).isEqualTo("http://localhost:8080/api/v1/tokens/redeem");
            assertThat(request.cookies()).hasSize(1);
            assertThat(request.cookies().getFirst("cg_user_id")).isEqualTo(user.toString());
            return Mono.just(response(200, "{\"queueToken\":\"" + claims.gateId() + ":"
                    + claims.sessionId() + "\",\"status\":\"ACTIVE\"}"));
        });
        new DemoActivationClient(builder, "http://localhost:8080").activate(claims, "signed", user)
                .block(Duration.ofSeconds(4));
        rejects(200, "{\"queueToken\":\"another\",\"status\":\"ACTIVE\"}", 502);
        rejects(200, "{\"status\":\"WAITING\"}", 502);
        rejects(200, "", 502);
    }

    @Test
    void propagatesRejectionsAndFailsClosedOnUnavailableCrowdGate() {
        for (int status : new int[]{401, 403, 404, 409, 410, 500, 503}) {
            rejects(status, "{}", status >= 500 ? 503 : status);
        }
        var never = WebClient.builder().exchangeFunction(request -> Mono.never());
        assertThatThrownBy(() -> new DemoActivationClient(never, "http://localhost:8080")
                .activate(claims, "signed", user).block(Duration.ofSeconds(4)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(503));
        for (String url : new String[]{"file:///tmp", "http://user@localhost", "http://localhost/path",
                "http://localhost?secret=value", "http://localhost#fragment"}) {
            assertThatThrownBy(() -> new DemoActivationClient(WebClient.builder(), url))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private void rejects(int upstream, String body, int expected) {
        var builder = WebClient.builder().exchangeFunction(request -> Mono.just(response(upstream, body)));
        assertThatThrownBy(() -> new DemoActivationClient(builder, "http://localhost:8080")
                .activate(claims, "signed", user).block(Duration.ofSeconds(4)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(expected));
    }

    private ClientResponse response(int status, String body) {
        var response = ClientResponse.create(HttpStatusCode.valueOf(status)).header("Content-Type", "application/json");
        return body.isEmpty() ? response.build() : response.body(body).build();
    }
}
