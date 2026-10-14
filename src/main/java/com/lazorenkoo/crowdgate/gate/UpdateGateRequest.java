package com.lazorenkoo.crowdgate.gate;

public record UpdateGateRequest(
        Long version,
        String name,
        Integer admissionRate,
        Integer maxActiveUsers,
        Integer sessionTtlSeconds,
        Integer tokenTtlSeconds
) {
    public CreateGateRequest merge(Gate gate) {
        return new CreateGateRequest(
                name == null ? gate.getName() : name,
                admissionRate == null ? gate.getAdmissionRate() : admissionRate,
                maxActiveUsers == null ? gate.getMaxActiveUsers() : maxActiveUsers,
                sessionTtlSeconds == null ? gate.getSessionTtlSeconds() : sessionTtlSeconds,
                tokenTtlSeconds == null ? gate.getTokenTtlSeconds() : tokenTtlSeconds);
    }
}
