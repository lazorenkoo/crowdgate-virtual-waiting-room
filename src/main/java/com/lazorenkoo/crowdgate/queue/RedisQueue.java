package com.lazorenkoo.crowdgate.queue;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Component
public class RedisQueue {

    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<List> joinScript;
    private final DefaultRedisScript<List> statusScript;
    private final DefaultRedisScript<Long> closeScript;
    private final DefaultRedisScript<List> admitScript;

    public RedisQueue(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
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
    }

    public Mono<QueueEntry> join(UUID gateId, UUID userId) {
        return Mono.defer(() -> {
            String token = gateId + ":" + UUID.randomUUID();
            return redis.execute(joinScript, keys(gateId), List.of(userId.toString(), token))
                    .single().flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.GONE, "Gate is closed"))
                            : Mono.just(entry(result)));
        });
    }

    public Mono<Void> close(UUID gateId) {
        return redis.execute(closeScript, keys(gateId)).then();
    }

    public Mono<List<String>> admit(UUID gateId, int limit, int capacity, int tokenTtlSeconds) {
        return Mono.defer(() -> {
            if (limit < 1 || capacity < 1 || tokenTtlSeconds < 1) {
                return Mono.error(new IllegalArgumentException("Admission limits must be positive"));
            }
            return redis.execute(admitScript, keys(gateId), List.of(Integer.toString(limit),
                            Integer.toString(capacity), Integer.toString(tokenTtlSeconds)))
                    .single().map(result -> {
                        List<?> values = result;
                        return values.stream().map(Object::toString).toList();
                    });
        });
    }

    private List<String> keys(UUID gateId) {
        String prefix = "cg:{" + gateId + "}:";
        return List.of(prefix + "waiting", prefix + "identities", prefix + "statuses",
                prefix + "owners", prefix + "sequence", prefix + "closed", prefix + "reservations",
                prefix + "admission-window", prefix + "active");
    }

    public Mono<QueueEntry> status(String token, UUID userId) {
        return Mono.defer(() -> {
            UUID gateId = gateId(token);
            return redis.execute(statusScript, keys(gateId), List.of(token, userId.toString()))
                    .single().flatMap(result -> result.isEmpty()
                            ? Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Queue entry not found"))
                            : Mono.just(entry(result)));
        });
    }

    private UUID gateId(String token) {
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
