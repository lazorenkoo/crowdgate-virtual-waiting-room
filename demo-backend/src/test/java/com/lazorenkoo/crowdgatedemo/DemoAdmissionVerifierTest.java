package com.lazorenkoo.crowdgatedemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DemoAdmissionVerifierTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final String key = "demo-test-signing-key-with-at-least-32-bytes";
    private final UUID gate = UUID.randomUUID();
    private final AdmissionTokenCodec codec = new AdmissionTokenCodec(mapper, key);
    private final DemoAdmissionVerifier verifier = new DemoAdmissionVerifier(mapper, key, gate);

    @Test
    void verifiesSignedClaimsWithoutAnExternalService() {
        Instant now = Instant.now().minusSeconds(1);
        AdmissionClaims claims = new AdmissionClaims(gate, UUID.randomUUID(), now,
                now.plusSeconds(60), UUID.randomUUID());
        assertThat(verifier.verify("Bearer " + codec.sign(claims))).isEqualTo(claims);
        for (String header : new String[]{null, "", "Basic credentials", "Bearer invalid", "bearer invalid"}) {
            rejects(header, 401);
        }
        rejects("Bearer " + codec.sign(claims) + "x", 401);
        var wrongKey = new AdmissionTokenCodec(mapper, "another-demo-test-key-with-at-least-32-bytes");
        rejects("Bearer " + wrongKey.sign(claims), 401);
    }

    @Test
    void rejectsExpiredAdmissionAndAnotherGate() {
        Instant now = Instant.now().minusSeconds(120);
        AdmissionClaims expired = new AdmissionClaims(gate, UUID.randomUUID(), now,
                now.plusSeconds(60), UUID.randomUUID());
        rejects("Bearer " + codec.sign(expired), 401);
        AdmissionClaims another = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
                Instant.now().minusSeconds(1), Instant.now().plusSeconds(60), UUID.randomUUID());
        rejects("Bearer " + codec.sign(another), 403);
        assertThatThrownBy(() -> new DemoAdmissionVerifier(mapper, "short", gate))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void rejects(String authorization, int status) {
        assertThatThrownBy(() -> verifier.verify(authorization)).isInstanceOfSatisfying(
                ResponseStatusException.class, error -> assertThat(error.getStatusCode().value()).isEqualTo(status));
    }
}
