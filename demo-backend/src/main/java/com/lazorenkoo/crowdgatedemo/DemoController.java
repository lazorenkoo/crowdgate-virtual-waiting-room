package com.lazorenkoo.crowdgatedemo;

import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebSession;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final DemoAdmissionVerifier verifier;
    private final DemoActivationClient activation;

    public DemoController(DemoAdmissionVerifier verifier, DemoActivationClient activation) {
        this.verifier = verifier;
        this.activation = activation;
    }

    @PostMapping("/enter")
    public Mono<ProtectedView> enter(ServerWebExchange exchange, WebSession session) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        String authorization = exchange.getRequest().getHeaders().getFirst("Authorization");
        AdmissionClaims claims = verifier.verify(authorization);
        UUID user = user(exchange);
        if (claims.equals(session.getAttribute("admission")) && user.equals(session.getAttribute("user"))) {
            return Mono.just(new ProtectedView(claims.gateId(), claims.sessionId()));
        }
        return activation.activate(claims, authorization.substring(7), user).then(Mono.fromSupplier(() -> {
            verifier.verify(authorization);
            session.getAttributes().put("admission", claims);
            session.getAttributes().put("user", user);
            session.setMaxIdleTime(Duration.between(Instant.now(), claims.expiresAt()));
            session.start();
            return new ProtectedView(claims.gateId(), claims.sessionId());
        }));
    }

    @GetMapping("/protected")
    public Mono<ProtectedView> protectedContent(ServerWebExchange exchange, WebSession session) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        AdmissionClaims claims = verifier.verify(exchange.getRequest().getHeaders().getFirst("Authorization"));
        if (!claims.equals(session.getAttribute("admission")) || !user(exchange).equals(session.getAttribute("user"))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Admission has not been activated");
        }
        return Mono.just(new ProtectedView(claims.gateId(), claims.sessionId()));
    }

    private UUID user(ServerWebExchange exchange) {
        var cookie = exchange.getRequest().getCookies().getFirst("cg_user_id");
        try {
            UUID id = cookie == null ? null : UUID.fromString(cookie.getValue());
            if (id == null || !id.toString().equals(cookie.getValue())) {
                throw new IllegalArgumentException();
            }
            return id;
        } catch (IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User identity is required");
        }
    }

    public record ProtectedView(UUID gateId, UUID sessionId) {
    }
}
