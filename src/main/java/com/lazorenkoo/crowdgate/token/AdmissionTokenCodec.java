package com.lazorenkoo.crowdgate.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

public class AdmissionTokenCodec {

    private final ObjectMapper mapper;
    private final SecretKeySpec key;
    private final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    public AdmissionTokenCodec(ObjectMapper mapper, String secret) {
        byte[] bytes = secret.getBytes(StandardCharsets.UTF_8);
        if (secret.isBlank() || bytes.length < 32) {
            throw new IllegalArgumentException("Admission signing key must contain at least 32 bytes");
        }
        this.mapper = mapper;
        this.key = new SecretKeySpec(bytes, "HmacSHA256");
    }

    public String sign(AdmissionClaims claims) {
        try {
            String payload = encoder.encodeToString(mapper.writeValueAsBytes(claims));
            return payload + "." + encoder.encodeToString(signature(payload));
        } catch (java.io.IOException error) {
            throw new IllegalStateException("Cannot encode admission token", error);
        }
    }

    public AdmissionClaims verify(String token) {
        try {
            if (token == null || token.length() > 2048) {
                throw new IllegalArgumentException();
            }
            String[] parts = token.split("\\.", -1);
            if (parts.length != 2 || !encoder.encodeToString(Base64.getUrlDecoder().decode(parts[1]))
                    .equals(parts[1]) || !MessageDigest.isEqual(signature(parts[0]), Base64.getUrlDecoder().decode(parts[1]))) {
                throw new IllegalArgumentException();
            }
            AdmissionClaims claims = mapper.readValue(Base64.getUrlDecoder().decode(parts[0]), AdmissionClaims.class);
            Instant now = Instant.now();
            if (claims == null || claims.gateId() == null || claims.sessionId() == null || claims.nonce() == null
                    || claims.issuedAt() == null || claims.expiresAt() == null
                    || claims.issuedAt().isAfter(now) || !claims.expiresAt().isAfter(now)
                    || !claims.expiresAt().isAfter(claims.issuedAt())) {
                throw new IllegalArgumentException();
            }
            return claims;
        } catch (java.io.IOException | IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid admission token");
        }
    }

    private byte[] signature(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(key);
            return mac.doFinal(payload.getBytes(StandardCharsets.US_ASCII));
        } catch (GeneralSecurityException error) {
            throw new IllegalStateException("Cannot sign admission token", error);
        }
    }
}
