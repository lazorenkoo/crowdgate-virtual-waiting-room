package com.lazorenkoo.crowdgate.admin;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Component
public class AdminAccessFilter implements WebFilter {

    private final byte[] key;

    public AdminAccessFilter(@Value("${crowdgate.admin.key}") String key) {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Admin key must not be blank");
        }
        this.key = key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().pathWithinApplication().value();
        boolean protectedPath = path.equals("/api/v1/admin") || path.startsWith("/api/v1/admin/")
                || path.equals("/actuator/prometheus") || path.startsWith("/actuator/prometheus/");
        if (!protectedPath) {
            return chain.filter(exchange);
        }
        String provided = exchange.getRequest().getHeaders().getFirst("X-Admin-Key");
        if (provided == null || !MessageDigest.isEqual(key, provided.getBytes(StandardCharsets.UTF_8))) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Admin key is required"));
        }
        return chain.filter(exchange);
    }
}
