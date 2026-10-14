package com.lazorenkoo.crowdgate.queue;

import org.springframework.http.HttpCookie;
import org.springframework.http.ResponseCookie;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import java.time.Duration;
import java.util.UUID;

@Component
public class UserIdentity {

    public UUID resolve(ServerWebExchange exchange) {
        HttpCookie cookie = exchange.getRequest().getCookies().getFirst("cg_user_id");
        if (cookie != null) {
            UUID existing = parse(cookie.getValue());
            if (existing != null) {
                return existing;
            }
        }
        UUID id = UUID.randomUUID();
        boolean secure = "https".equalsIgnoreCase(exchange.getRequest().getURI().getScheme());
        ResponseCookie replacement = ResponseCookie.from("cg_user_id", id.toString())
                .httpOnly(true)
                .secure(secure)
                .sameSite("Lax")
                .path("/")
                .maxAge(Duration.ofDays(365))
                .build();
        exchange.getResponse().addCookie(replacement);
        return id;
    }

    private UUID parse(String value) {
        try {
            UUID id = UUID.fromString(value);
            return id.toString().equalsIgnoreCase(value) ? id : null;
        } catch (IllegalArgumentException error) {
            return null;
        }
    }
}
