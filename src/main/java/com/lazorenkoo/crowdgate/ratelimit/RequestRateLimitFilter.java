package com.lazorenkoo.crowdgate.ratelimit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
@Order(-100)
public class RequestRateLimitFilter implements WebFilter {

    private static final UUID SCOPE = new UUID(0, 0);
    private final RedisRateLimiter limiter;
    private final int capacity;
    private final int refill;

    public RequestRateLimitFilter(RedisRateLimiter limiter,
                                 @Value("${crowdgate.requests.burst:20}") int capacity,
                                 @Value("${crowdgate.requests.refill:10}") int refill) {
        if (capacity < 1 || refill < 1) {
            throw new IllegalArgumentException("Request limits must be positive");
        }
        this.limiter = limiter;
        this.capacity = capacity;
        this.refill = refill;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!path.startsWith("/api/v1/") || path.startsWith("/api/v1/admin/")) {
            return chain.filter(exchange);
        }
        var remote = exchange.getRequest().getRemoteAddress();
        String address = remote == null ? "unknown" : remote.getAddress().getHostAddress();
        UUID client = UUID.nameUUIDFromBytes(address.getBytes(StandardCharsets.UTF_8));
        return limiter.acquire(SCOPE, client, capacity, refill).onErrorReturn(-1L).flatMap(delay -> {
            if (delay == 0) {
                return chain.filter(exchange);
            }
            exchange.getResponse().getHeaders().setCacheControl("no-store");
            exchange.getResponse().setStatusCode(delay < 0
                    ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.TOO_MANY_REQUESTS);
            if (delay > 0) {
                exchange.getResponse().getHeaders().set("Retry-After", Long.toString((delay + 999) / 1000));
            }
            return exchange.getResponse().setComplete();
        });
    }
}
