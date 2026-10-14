package com.lazorenkoo.crowdgate.queue;

public record QueueEntry(String queueToken, String status, long position, long queueSize) {
}
