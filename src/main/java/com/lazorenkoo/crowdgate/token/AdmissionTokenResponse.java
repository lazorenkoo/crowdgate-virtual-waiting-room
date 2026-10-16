package com.lazorenkoo.crowdgate.token;

import java.time.Instant;

public record AdmissionTokenResponse(String admissionToken, Instant expiresAt) {
}
