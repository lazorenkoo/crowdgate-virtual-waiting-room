package com.lazorenkoo.crowdgate.queue;

import com.lazorenkoo.crowdgate.gate.GateService;
import com.lazorenkoo.crowdgate.gate.GateStatus;
import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import com.lazorenkoo.crowdgate.token.AdmissionTokenCodec;
import com.lazorenkoo.crowdgate.token.AdmissionTokenRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.HttpCookie;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.Duration;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class QueueController {

    private final GateService gates;
    private final RedisQueue queue;
    private final UserIdentity identity;
    private final AdmissionTokenCodec tokens;

    public QueueController(GateService gates, RedisQueue queue, UserIdentity identity, AdmissionTokenCodec tokens) {
        this.gates = gates;
        this.queue = queue;
        this.identity = identity;
        this.tokens = tokens;
    }

    @PostMapping("/gates/{gateId}/join")
    public Mono<QueueEntry> join(@PathVariable UUID gateId, ServerWebExchange exchange) {
        return gates.findById(gateId).flatMap(gate -> {
            if (gate.getStatus() == GateStatus.CLOSED || gate.getClosesAt() != null
                    && !gate.getClosesAt().isAfter(Instant.now())) {
                return Mono.error(new ResponseStatusException(HttpStatus.GONE, "Gate is closed"));
            }
            if (gate.getStatus() != GateStatus.OPEN && gate.getStatus() != GateStatus.PAUSED) {
                return Mono.error(new ResponseStatusException(HttpStatus.CONFLICT, "Gate is not open"));
            }
            return queue.join(gateId, identity.resolve(exchange));
        });
    }

    @GetMapping("/queue/{queueToken}")
    public Mono<QueueEntry> status(@PathVariable String queueToken, ServerWebExchange exchange) {
        return queue.status(queueToken, identity.resolve(exchange));
    }

    @PostMapping("/queue/{queueToken}/redeem")
    public Mono<QueueEntry> redeem(@PathVariable String queueToken, ServerWebExchange exchange) {
        UUID user = identity.resolve(exchange);
        return redeem(queueToken, user, null, exchange);
    }

    @PostMapping("/tokens/redeem")
    public Mono<QueueEntry> redeemToken(@RequestBody AdmissionTokenRequest request, ServerWebExchange exchange) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        return Mono.defer(() -> {
            AdmissionClaims claims = tokens.verify(request.admissionToken());
            return redeem(claims.gateId() + ":" + claims.sessionId(), identity.resolve(exchange), claims, exchange);
        });
    }

    private Mono<QueueEntry> redeem(String queueToken, UUID user, AdmissionClaims claims, ServerWebExchange exchange) {
        return queue.status(queueToken, user)
                .flatMap(entry -> gates.findById(queue.gateId(queueToken)))
                .flatMap(gate -> {
                    boolean closed = gate.getStatus() == GateStatus.CLOSED || gate.getClosesAt() != null
                            && !gate.getClosesAt().isAfter(Instant.now());
                    Mono<Void> synchronize = closed ? queue.close(gate.getId()) : Mono.empty();
                    Mono<QueueEntry> activation = claims == null
                            ? queue.redeem(queueToken, user, gate.getSessionTtlSeconds())
                            : queue.redeemSigned(claims, user, gate.getSessionTtlSeconds());
                    return synchronize.then(activation)
                            .flatMap(entry -> queue.issueSession(queueToken, user).map(session -> {
                                setSessionCookie(exchange, gate.getId(), session);
                                return entry;
                            }));
                });
    }

    @GetMapping("/gates/{gateId}/session")
    public Mono<SessionView> session(@PathVariable UUID gateId, ServerWebExchange exchange) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        return Mono.defer(() -> {
            HttpCookie cookie = exchange.getRequest().getCookies().getFirst("cg_session");
            if (cookie == null || !queue.sessionGateId(cookie.getValue()).equals(gateId)) {
                return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid session"));
            }
            return queue.validateSession(cookie.getValue(), identity.resolve(exchange))
                    .map(session -> new SessionView(gateId, session.expiresAtMillis()));
        });
    }

    @PostMapping("/gates/{gateId}/session/heartbeat")
    public Mono<SessionView> heartbeat(@PathVariable UUID gateId, ServerWebExchange exchange) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        return Mono.defer(() -> {
            HttpCookie cookie = exchange.getRequest().getCookies().getFirst("cg_session");
            if (cookie == null || !queue.sessionGateId(cookie.getValue()).equals(gateId)) {
                return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid session"));
            }
            UUID user = identity.resolve(exchange);
            return queue.validateSession(cookie.getValue(), user)
                    .flatMap(session -> gates.findById(gateId))
                    .flatMap(gate -> queue.heartbeat(cookie.getValue(), user, gate.getSessionTtlSeconds()))
                    .map(session -> {
                        setSessionCookie(exchange, gateId, session);
                        return new SessionView(gateId, session.expiresAtMillis());
                    });
        });
    }

    private void setSessionCookie(ServerWebExchange exchange, UUID gateId, QueueSession session) {
        long remaining = Math.max(0, session.expiresAtMillis() - Instant.now().toEpochMilli());
        ResponseCookie cookie = ResponseCookie.from("cg_session", session.sessionToken())
                .httpOnly(true).sameSite("Lax")
                .secure("https".equalsIgnoreCase(exchange.getRequest().getURI().getScheme()))
                .path("/api/v1/gates/" + gateId)
                .maxAge(Duration.ofMillis(remaining)).build();
        exchange.getResponse().addCookie(cookie);
        exchange.getResponse().getHeaders().setCacheControl("no-store");
    }
}
