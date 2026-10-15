package com.lazorenkoo.crowdgate.queue;

import java.util.UUID;

public record SessionView(UUID gateId, long expiresAtMillis) {
}
