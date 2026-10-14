package com.lazorenkoo.crowdgate.config;

public record ApiError(int status, String message, String path) {
}
