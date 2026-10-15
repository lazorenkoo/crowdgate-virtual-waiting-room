package com.lazorenkoo.crowdgate.queue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class RedisQueueTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7.4").withExposedPorts(6379);
    static LettuceConnectionFactory connection;
    static ReactiveStringRedisTemplate template;
    static RedisQueue queue;

    @BeforeAll
    static void connect() {
        connection = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connection.afterPropertiesSet();
        connection.start();
        template = new ReactiveStringRedisTemplate(connection);
        queue = new RedisQueue(template);
    }

    @AfterAll
    static void disconnect() {
        connection.destroy();
    }

    @Test
    void concurrentJoinsCannotRecreateWaitingQueueAfterClose() {
        UUID gate = UUID.randomUUID();
        UUID seedUser = UUID.randomUUID();
        QueueEntry seed = queue.join(gate, seedUser).block(Duration.ofSeconds(5));
        RedisQueue second = new RedisQueue(template);
        var accepted = Flux.range(0, 1001).flatMap(index -> {
            if (index == 500) {
                return second.close(gate).then(Mono.<java.util.Map.Entry<UUID, QueueEntry>>empty());
            }
            UUID user = UUID.randomUUID();
            RedisQueue instance = index % 2 == 0 ? queue : second;
            return instance.join(gate, user).map(entry -> java.util.Map.entry(user, entry))
                    .onErrorResume(ResponseStatusException.class, error ->
                            error.getStatusCode().value() == 410 ? Mono.empty() : Mono.error(error));
        }, 64).collectList().block(Duration.ofSeconds(30));

        assertThat(queue.status(seed.queueToken(), seedUser).block(Duration.ofSeconds(5)).status())
                .isEqualTo("CANCELLED");
        var statuses = Flux.fromIterable(accepted)
                .flatMap(entry -> queue.status(entry.getValue().queueToken(), entry.getKey()), 64)
                .collectList().block(Duration.ofSeconds(30));
        assertThat(statuses).hasSize(accepted.size()).allSatisfy(entry -> {
            assertThat(entry.status()).isEqualTo("CANCELLED");
            assertThat(entry.position()).isZero();
            assertThat(entry.queueSize()).isZero();
        });
        assertThat(template.opsForZSet().size("cg:{" + gate + "}:waiting").block(Duration.ofSeconds(5)))
                .isZero();
        assertThatThrownBy(() -> second.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(410));
    }

    @Test
    void closesWaitingAndAdmittedEntriesWithoutRevokingActiveOnes() {
        UUID gate = UUID.randomUUID();
        String prefix = "cg:{" + gate + "}:";
        for (String state : new String[]{"WAITING", "ADMITTED", "ACTIVE"}) {
            UUID user = UUID.randomUUID();
            QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
            template.opsForHash().put(prefix + "statuses", entry.queueToken(), state)
                    .block(Duration.ofSeconds(5));
            if (!state.equals("WAITING")) {
                template.opsForZSet().remove(prefix + "waiting", entry.queueToken())
                        .block(Duration.ofSeconds(5));
            }
        }
        var owners = template.<String, String>opsForHash().entries(prefix + "owners")
                .collectList().block(Duration.ofSeconds(5));
        queue.close(gate).block(Duration.ofSeconds(5));
        queue.close(gate).block(Duration.ofSeconds(5));
        int cancelled = 0;
        int active = 0;
        for (var owner : owners) {
            QueueEntry entry = queue.status(owner.getKey(), UUID.fromString(owner.getValue()))
                    .block(Duration.ofSeconds(5));
            assertThat(entry.position()).isZero();
            assertThat(entry.queueSize()).isZero();
            if (entry.status().equals("CANCELLED")) {
                cancelled++;
            } else if (entry.status().equals("ACTIVE")) {
                active++;
            }
        }
        assertThat(cancelled).isEqualTo(2);
        assertThat(active).isEqualTo(1);
        assertThatThrownBy(() -> queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(410));
    }

    @Test
    void queuesThousandConcurrentUsersWithoutMissingPositions() {
        UUID gate = UUID.randomUUID();
        var entries = Flux.range(0, 1000)
                .flatMap(index -> queue.join(gate, UUID.randomUUID()), 64)
                .collectList().block(Duration.ofSeconds(30));
        assertThat(entries).hasSize(1000);
        assertThat(entries.stream().map(QueueEntry::queueToken).distinct().count()).isEqualTo(1000);
        var positions = entries.stream().map(QueueEntry::position).sorted().toList();
        assertThat(positions).containsExactlyElementsOf(
                java.util.stream.LongStream.rangeClosed(1, 1000).boxed().toList());
        String prefix = "cg:{" + gate + "}:";
        assertThat(template.opsForZSet().size(prefix + "waiting").block(Duration.ofSeconds(5)))
                .isEqualTo(1000);
        assertThat(template.opsForHash().size(prefix + "identities").block(Duration.ofSeconds(5)))
                .isEqualTo(1000);
        assertThat(template.opsForHash().size(prefix + "owners").block(Duration.ofSeconds(5)))
                .isEqualTo(1000);
    }

    @Test
    void concurrentInstancesReuseOneEntryForSameIdentity() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        RedisQueue secondInstance = new RedisQueue(template);
        var entries = Flux.range(0, 1000)
                .flatMap(index -> (index % 2 == 0 ? queue : secondInstance).join(gate, user), 64)
                .collectList().block(Duration.ofSeconds(30));
        assertThat(entries).hasSize(1000);
        assertThat(entries.stream().map(QueueEntry::queueToken).distinct().count()).isEqualTo(1);
        assertThat(entries).allSatisfy(entry -> {
            assertThat(entry.position()).isEqualTo(1);
            assertThat(entry.queueSize()).isEqualTo(1);
            assertThat(entry.status()).isEqualTo("WAITING");
        });
        String prefix = "cg:{" + gate + "}:";
        assertThat(template.opsForValue().get(prefix + "sequence").block(Duration.ofSeconds(5)))
                .isEqualTo("1");
        QueueEntry next = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        assertThat(next.position()).isEqualTo(2);
        assertThat(next.queueSize()).isEqualTo(2);
    }

    @Test
    void readsCurrentPositionOnlyForEntryOwner() {
        UUID gate = UUID.randomUUID();
        UUID firstUser = UUID.randomUUID();
        UUID secondUser = UUID.randomUUID();
        QueueEntry first = queue.join(gate, firstUser).block(Duration.ofSeconds(5));
        QueueEntry second = queue.join(gate, secondUser).block(Duration.ofSeconds(5));
        QueueEntry status = queue.status(second.queueToken(), secondUser).block(Duration.ofSeconds(5));
        assertThat(status.status()).isEqualTo("WAITING");
        assertThat(status.position()).isEqualTo(2);
        assertThat(status.queueSize()).isEqualTo(2);
        template.opsForZSet().remove("cg:{" + gate + "}:waiting", first.queueToken())
                .block(Duration.ofSeconds(5));
        assertThat(queue.status(second.queueToken(), secondUser).block(Duration.ofSeconds(5)).position())
                .isEqualTo(1);
        assertThatThrownBy(() -> queue.status(second.queueToken(), firstUser).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
        assertThatThrownBy(() -> queue.status(gate + ":" + UUID.randomUUID(), secondUser)
                .block(Duration.ofSeconds(5))).isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
    }

    @Test
    void rejectsMalformedQueueTokens() {
        for (String token : new String[]{"invalid", "1-1-1-1-1:" + UUID.randomUUID(),
                UUID.randomUUID() + ":invalid", UUID.randomUUID() + ":" + UUID.randomUUID() + ":extra"}) {
            assertThatThrownBy(() -> queue.status(token, UUID.randomUUID()).block(Duration.ofSeconds(5)))
                    .isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode().value()).isEqualTo(400));
        }
    }

    @Test
    void preservesFifoAndReusesIdentityWithinGate() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
        QueueEntry second = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        QueueEntry repeated = queue.join(gate, user).block(Duration.ofSeconds(5));
        assertThat(first.position()).isEqualTo(1);
        assertThat(second.position()).isEqualTo(2);
        assertThat(repeated.queueToken()).isEqualTo(first.queueToken());
        assertThat(repeated.queueSize()).isEqualTo(2);
        assertThat(repeated.position()).isEqualTo(1);
        assertThat(queue.join(UUID.randomUUID(), user).block(Duration.ofSeconds(5)).queueToken())
                .isNotEqualTo(first.queueToken());
    }

    @Test
    void reusesAdmittedAndActiveEntriesWithoutRejoiningWaitingQueue() {
        for (String state : new String[]{"ADMITTED", "ACTIVE"}) {
            UUID gate = UUID.randomUUID();
            UUID user = UUID.randomUUID();
            QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
            String prefix = "cg:{" + gate + "}:";
            template.opsForZSet().remove(prefix + "waiting", entry.queueToken()).block(Duration.ofSeconds(5));
            template.opsForHash().put(prefix + "statuses", entry.queueToken(), state).block(Duration.ofSeconds(5));
            QueueEntry repeated = queue.join(gate, user).block(Duration.ofSeconds(5));
            assertThat(repeated.queueToken()).isEqualTo(entry.queueToken());
            assertThat(repeated.status()).isEqualTo(state);
            assertThat(repeated.position()).isZero();
            assertThat(repeated.queueSize()).isZero();
        }
    }
}
