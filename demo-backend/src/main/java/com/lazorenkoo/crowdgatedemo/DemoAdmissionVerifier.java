package com.lazorenkoo.crowdgatedemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Service
public class DemoAdmissionVerifier {

    private final AdmissionTokenCodec codec;
    private final UUID gateId;

    public DemoAdmissionVerifier(ObjectMapper mapper,
            @Value("${crowdgate.demo.signing-key}") String secret,
            @Value("${crowdgate.demo.gate-id}") UUID gateId) {
        this.codec = new AdmissionTokenCodec(mapper, secret);
        this.gateId = gateId;
    }

    public AdmissionClaims verify(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Admission token is required");
        }
        AdmissionClaims claims = codec.verify(authorization.substring(7));
        if (!gateId.equals(claims.gateId())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admission belongs to another gate");
        }
        return claims;
    }
}
