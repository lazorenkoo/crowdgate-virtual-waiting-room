package com.lazorenkoo.crowdgate.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdmissionTokenCodecTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final AdmissionTokenCodec codec = new AdmissionTokenCodec(mapper, "test-signing-key-32-bytes-minimum-value");

    @Test
    void verifiesClaimsLocallyAndRejectsPayloadOrSignatureTampering() {
        Instant now = Instant.now().minusSeconds(1);
        AdmissionClaims claims = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
                now, now.plusSeconds(60), UUID.randomUUID());
        String token = codec.sign(claims);
        assertThat(codec.verify(token)).isEqualTo(claims);
        String[] parts = token.split("\\.");
        String payload = (parts[0].charAt(0) == 'A' ? "B" : "A") + parts[0].substring(1);
        String signature = (parts[1].charAt(0) == 'A' ? "B" : "A") + parts[1].substring(1);
        rejects(payload + "." + parts[1]);
        rejects(parts[0] + "." + signature);
        rejects(parts[0] + "." + parts[1] + "=");
        var other = new AdmissionTokenCodec(mapper, "different-test-key-with-at-least-32-bytes");
        rejects(other.sign(claims));
    }

    @Test
    void rejectsExpiredFutureAndIncompleteClaimsEvenWithValidSignature() {
        Instant now = Instant.now();
        UUID gate = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        UUID nonce = UUID.randomUUID();
        for (AdmissionClaims claims : new AdmissionClaims[]{
                new AdmissionClaims(gate, session, now.minusSeconds(60), now.minusSeconds(1), nonce),
                new AdmissionClaims(gate, session, now.plusSeconds(60), now.plusSeconds(120), nonce),
                new AdmissionClaims(gate, session, now.minusSeconds(1), now.minusSeconds(2), nonce),
                new AdmissionClaims(null, session, now, now.plusSeconds(60), nonce),
                new AdmissionClaims(gate, null, now, now.plusSeconds(60), nonce),
                new AdmissionClaims(gate, session, now, now.plusSeconds(60), null),
                new AdmissionClaims(gate, session, null, now.plusSeconds(60), nonce),
                new AdmissionClaims(gate, session, now, null, nonce)}) {
            rejects(codec.sign(claims));
        }
    }

    @Test
    void rejectsMalformedTokensAndShortSigningKeys() {
        for (String token : new String[]{null, "", "invalid", ".", "a.b.c", "!!!.???", "x".repeat(2049)}) {
            rejects(token);
        }
        assertThatThrownBy(() -> new AdmissionTokenCodec(mapper, "short"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void rejects(String token) {
        assertThatThrownBy(() -> codec.verify(token)).isInstanceOfSatisfying(ResponseStatusException.class, error -> {
            assertThat(error.getStatusCode().value()).isEqualTo(401);
            assertThat(error.getReason()).isEqualTo("Invalid admission token");
        });
    }
}
