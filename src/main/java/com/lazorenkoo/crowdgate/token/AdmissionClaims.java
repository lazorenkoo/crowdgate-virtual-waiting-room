package com.lazorenkoo.crowdgate.token;

import java.time.Instant;
import java.util.UUID;

public record AdmissionClaims(UUID gateId, UUID sessionId, Instant issuedAt, Instant expiresAt, UUID nonce) {
}
