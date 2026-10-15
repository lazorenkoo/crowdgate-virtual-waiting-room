package com.lazorenkoo.crowdgate.queue;

public record QueueSession(String sessionToken, long expiresAtMillis) {
}
