package com.lazorenkoo.crowdgate.queue;

import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.Base64;
import java.security.SecureRandom;
import java.time.Instant;

@Component
public class RedisQueue {

    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<List> joinScript;
    private final DefaultRedisScript<List> statusScript;
    private final DefaultRedisScript<Long> closeScript;
    private final DefaultRedisScript<List> admitScript;
    private final DefaultRedisScript<List> redeemScript;
    private final DefaultRedisScript<List> issueSessionScript;
    private final DefaultRedisScript<List> validateSessionScript;
    private final DefaultRedisScript<List> readAdmissionScript;
    private final SecureRandom random = new SecureRandom();
    private final Counter joins;
    private final Counter admitted;
    private final MeterRegistry registry;

    public RedisQueue(ReactiveStringRedisTemplate redis) {
        this(redis, Metrics.globalRegistry);
    }

    @Autowired
    public RedisQueue(ReactiveStringRedisTemplate redis, MeterRegistry registry) {
        this.redis = redis;
        this.registry = registry;
        joins = Counter.builder("crowdgate.join").description("Successful join requests, including retries")
                .register(registry);
        admitted = Counter.builder("crowdgate.admitted").description("Queue entries admitted by this instance")
                .register(registry);
        joinScript = new DefaultRedisScript<>();
        joinScript.setLocation(new ClassPathResource("redis/join.lua"));
        joinScript.setResultType(List.class);
        statusScript = new DefaultRedisScript<>();
        statusScript.setLocation(new ClassPathResource("redis/status.lua"));
        statusScript.setResultType(List.class);
        closeScript = new DefaultRedisScript<>();
        closeScript.setLocation(new ClassPathResource("redis/close.lua"));
        closeScript.setResultType(Long.class);
        admitScript = new DefaultRedisScript<>();
        admitScript.setLocation(new ClassPathResource("redis/admit.lua"));
        admitScript.setResultType(List.class);
        redeemScript = new DefaultRedisScript<>();
        redeemScript.setLocation(new ClassPathResource("redis/redeem.lua"));
        redeemScript.setResultType(List.class);
        issueSessionScript = new DefaultRedisScript<>();
        issueSessionScript.setLocation(new ClassPathResource("redis/issue-session.lua"));
        issueSessionScript.setResultType(List.class);
        validateSessionScript = new DefaultRedisScript<>();
        validateSessionScript.setLocation(new ClassPathResource("redis/validate-session.lua"));
        validateSessionScript.setResultType(List.class);
        readAdmissionScript = new DefaultRedisScript<>();
        readAdmissionScript.setLocation(new ClassPathResource("redis/read-admission.lua"));
        readAdmissionScript.setResultType(List.class);
    }

    public Mono<QueueEntry> join(UUID gateId, UUID userId) {
        return Mono.defer(() -> {
            String token = gateId + ":" + UUID.randomUUID();
            return execute("join", joinScript, keys(gateId), List.of(userId.toString(), token))
                    .flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.GONE, "Gate is closed"))
                            : Mono.just(entry(result))).doOnNext(value -> joins.increment());
        });
    }

    public Mono<Void> close(UUID gateId) {
        return execute("close", closeScript, keys(gateId), List.of()).then();
    }

    public Mono<List<String>> admit(UUID gateId, int limit, int capacity, int tokenTtlSeconds) {
        return Mono.defer(() -> {
            if (limit < 1 || capacity < 1 || tokenTtlSeconds < 1) {
                return Mono.error(new IllegalArgumentException("Admission limits must be positive"));
            }
            return execute("admit", admitScript, keys(gateId), List.of(Integer.toString(limit),
                            Integer.toString(capacity), Integer.toString(tokenTtlSeconds)))
                    .map(result -> {
                        List<?> values = result;
                        return values.stream().map(Object::toString).toList();
                    }).doOnNext(values -> admitted.increment(values.size()));
        });
    }

    public Mono<QueueEntry> redeem(String token, UUID userId, int sessionTtlSeconds) {
        return redeem(token, userId, sessionTtlSeconds, null);
    }

    public Mono<QueueEntry> redeemSigned(AdmissionClaims claims, UUID userId, int sessionTtlSeconds) {
        if (claims.issuedAt().getNano() % 1_000_000 != 0 || claims.expiresAt().getNano() % 1_000_000 != 0) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid admission token"));
        }
        return redeem(claims.gateId() + ":" + claims.sessionId(), userId, sessionTtlSeconds, claims);
    }

    private Mono<QueueEntry> redeem(String token, UUID userId, int sessionTtlSeconds, AdmissionClaims claims) {
        return Mono.defer(() -> {
            UUID gateId = gateId(token);
            if (sessionTtlSeconds < 1) {
                return Mono.error(new IllegalArgumentException("Session TTL must be positive"));
            }
            List<String> arguments = claims == null
                    ? List.of(token, userId.toString(), Integer.toString(sessionTtlSeconds))
                    : List.of(token, userId.toString(), Integer.toString(sessionTtlSeconds),
                            claims.nonce().toString(), Long.toString(claims.issuedAt().toEpochMilli()),
                            Long.toString(claims.expiresAt().toEpochMilli()));
            return execute("redeem", redeemScript, keys(gateId), arguments)
                    .map(result -> {
                        if (result.size() == 1) {
                            int code = Integer.parseInt(result.getFirst().toString());
                            String message = code == 404 ? "Queue entry not found"
                                    : code == 410 ? "Gate is closed" : "Admission is not valid";
                            throw new ResponseStatusException(HttpStatus.valueOf(code), message);
                        }
                        return entry(result);
                    });
        });
    }

    public Mono<QueueSession> issueSession(String token, UUID userId) {
        return Mono.defer(() -> {
            UUID gate = gateId(token);
            byte[] bytes = new byte[32];
            random.nextBytes(bytes);
            String candidate = gate + ":" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
            return execute("session.issue", issueSessionScript, keys(gate), List.of(token, userId.toString(), candidate))
                    .flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Active session not found"))
                            : Mono.just(new QueueSession(result.get(0).toString(),
                                    Long.parseLong(result.get(1).toString()))));
        });
    }

    public Mono<QueueSession> validateSession(String token, UUID userId) {
        return session(token, userId, null);
    }

    public Mono<QueueSession> heartbeat(String token, UUID userId, int sessionTtlSeconds) {
        if (sessionTtlSeconds < 1) {
            return Mono.error(new IllegalArgumentException("Session TTL must be positive"));
        }
        return session(token, userId, sessionTtlSeconds);
    }

    private Mono<QueueSession> session(String token, UUID userId, Integer ttl) {
        return Mono.defer(() -> {
            UUID gate = sessionGateId(token);
            List<String> arguments = ttl == null ? List.of(token, userId.toString())
                    : List.of(token, userId.toString(), ttl.toString());
            return execute(ttl == null ? "session.validate" : "session.heartbeat", validateSessionScript, keys(gate), arguments)
                    .flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid session"))
                            : Mono.just(new QueueSession(result.get(0).toString(),
                                    Long.parseLong(result.get(1).toString()))));
        });
    }

    public UUID sessionGateId(String token) {
        try {
            String[] parts = token.split(":", -1);
            if (parts.length != 2 || parts[1].length() != 43) {
                throw new IllegalArgumentException();
            }
            UUID gate = UUID.fromString(parts[0]);
            byte[] secret = Base64.getUrlDecoder().decode(parts[1]);
            if (!gate.toString().equals(parts[0]) || secret.length != 32
                    || !Base64.getUrlEncoder().withoutPadding().encodeToString(secret).equals(parts[1])) {
                throw new IllegalArgumentException();
            }
            return gate;
        } catch (IllegalArgumentException | NullPointerException error) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid session");
        }
    }

    public Mono<AdmissionClaims> admissionClaims(String token, UUID userId) {
        return Mono.defer(() -> {
            UUID gate = gateId(token);
            UUID session = UUID.fromString(token.substring(token.indexOf(':') + 1));
            return execute("admission.claims", readAdmissionScript, keys(gate),
                            List.of(token, userId.toString(), UUID.randomUUID().toString()))
                    .map(result -> {
                        if (result.size() == 1) {
                            int code = Integer.parseInt(result.getFirst().toString());
                            throw new ResponseStatusException(HttpStatus.valueOf(code), "Admission is not available");
                        }
                        return new AdmissionClaims(gate, session,
                                Instant.ofEpochMilli(Long.parseLong(result.get(0).toString())),
                                Instant.ofEpochMilli(Long.parseLong(result.get(1).toString())),
                                UUID.fromString(result.get(2).toString()));
                    });
        });
    }

    private List<String> keys(UUID gateId) {
        String prefix = "cg:{" + gateId + "}:";
        return List.of(prefix + "waiting", prefix + "identities", prefix + "statuses",
                prefix + "owners", prefix + "sequence", prefix + "closed", prefix + "reservations",
                prefix + "admission-window", prefix + "active", prefix + "sessions-by-entry",
                prefix + "session-entries", prefix + "admission-issued-at", prefix + "admission-nonces",
                prefix + "admission-expires-at");
    }

    public Mono<QueueEntry> status(String token, UUID userId) {
        return Mono.defer(() -> {
            UUID gateId = gateId(token);
            return execute("status", statusScript, keys(gateId), List.of(token, userId.toString()))
                    .flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Queue entry not found"))
                            : Mono.just(entry(result)));
        });
    }

    private <T> Mono<T> execute(String operation, RedisScript<T> script, List<String> keys, List<String> arguments) {
        return Mono.defer(() -> {
            Timer timer = registry.timer("crowdgate.redis.operation", "operation", operation);
            Timer.Sample sample = Timer.start(registry);
            return Mono.defer(() -> redis.execute(script, keys, arguments).single())
                    .doOnError(error -> registry.counter("crowdgate.redis.errors", "operation", operation).increment())
                    .doFinally(signal -> sample.stop(timer));
        });
    }

    public UUID gateId(String token) {
        String[] parts = token.split(":", -1);
        if (parts.length != 2) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid queue token");
        }
        try {
            UUID gateId = UUID.fromString(parts[0]);
            UUID entryId = UUID.fromString(parts[1]);
            if (!gateId.toString().equals(parts[0]) || !entryId.toString().equals(parts[1])) {
                throw new IllegalArgumentException();
            }
            return gateId;
        } catch (IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid queue token");
        }
    }

    private QueueEntry entry(List<?> result) {
        return new QueueEntry(result.get(0).toString(), result.get(1).toString(),
                Long.parseLong(result.get(2).toString()), Long.parseLong(result.get(3).toString()));
    }
}
