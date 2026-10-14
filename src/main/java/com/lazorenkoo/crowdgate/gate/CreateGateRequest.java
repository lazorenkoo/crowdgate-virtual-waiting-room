package com.lazorenkoo.crowdgate.gate;

public record CreateGateRequest(
        String name,
        int admissionRate,
        int maxActiveUsers,
        int sessionTtlSeconds,
        int tokenTtlSeconds
) {
}
