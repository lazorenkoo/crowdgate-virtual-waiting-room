package com.lazorenkoo.crowdgatedemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.*;

class DemoControllerTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final String key = "demo-controller-test-signing-key-at-least-32-bytes";
    private final UUID gate = UUID.randomUUID();
    private final UUID user = UUID.randomUUID();
    private final AdmissionClaims claims = new AdmissionClaims(gate, UUID.randomUUID(),
            Instant.now().minusSeconds(1), Instant.now().plusSeconds(120), UUID.randomUUID());
    private final String token = new AdmissionTokenCodec(mapper, key).sign(claims);
    private final DemoActivationClient activation = mock(DemoActivationClient.class);
    private final WebTestClient client = WebTestClient.bindToController(new DemoController(
            new DemoAdmissionVerifier(mapper, key, gate), activation)).build();

    @Test
    void activatesOnceAndChecksSubsequentRequestsLocally() {
        request("GET", "/protected", user, null, token).exchange().expectStatus().isUnauthorized();
        verifyNoInteractions(activation);
        when(activation.activate(claims, token, user)).thenReturn(Mono.empty());
        var result = request("POST", "/enter", user, null, token).exchange().expectStatus().isOk()
                .expectHeader().valueEquals("Cache-Control", "no-store")
                .expectBody().jsonPath("$.gateId").isEqualTo(gate.toString())
                .jsonPath("$.sessionToken").doesNotExist().returnResult();
        var cookie = result.getResponseCookies().getFirst("SESSION");
        org.assertj.core.api.Assertions.assertThat(cookie).isNotNull();
        for (int i = 0; i < 2; i++) {
            request("GET", "/protected", user, cookie.getValue(), token).exchange().expectStatus().isOk();
        }
        request("POST", "/enter", user, cookie.getValue(), token).exchange().expectStatus().isOk();
        verify(activation, times(1)).activate(claims, token, user);
        request("GET", "/protected", UUID.randomUUID(), cookie.getValue(), token)
                .exchange().expectStatus().isUnauthorized();
        request("GET", "/protected", user, cookie.getValue(), token + "x")
                .exchange().expectStatus().isUnauthorized();
        verifyNoMoreInteractions(activation);
    }

    @Test
    void failedActivationDoesNotGrantAccess() {
        when(activation.activate(claims, token, user)).thenReturn(Mono.error(
                new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE)));
        request("POST", "/enter", user, null, token).exchange().expectStatus().isEqualTo(503)
                .expectCookie().doesNotExist("SESSION");
        request("GET", "/protected", user, null, token).exchange().expectStatus().isUnauthorized();
        verify(activation).activate(claims, token, user);
        verifyNoMoreInteractions(activation);
    }

    private WebTestClient.RequestHeadersSpec<?> request(String method, String path, UUID owner,
                                                       String session, String credential) {
        var request = client.method(org.springframework.http.HttpMethod.valueOf(method))
                .uri("/api/demo" + path).header("Authorization", "Bearer " + credential)
                .cookie("cg_user_id", owner.toString());
        if (session != null) {
            request.cookie("SESSION", session);
        }
        return request;
    }
}
