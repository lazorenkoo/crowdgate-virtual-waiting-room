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
    void issuesSessionOnlyForOwnerOfLiveActiveEntry() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.issueSession(entry.queueToken(), user).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
        queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
        queue.redeem(entry.queueToken(), user, 60).block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.issueSession(entry.queueToken(), UUID.randomUUID())
                .block(Duration.ofSeconds(5))).isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
        QueueSession session = queue.issueSession(entry.queueToken(), user).block(Duration.ofSeconds(5));
        String[] parts = session.sessionToken().split(":");
        assertThat(parts).hasSize(2);
        assertThat(parts[0]).isEqualTo(gate.toString());
        assertThat(java.util.Base64.getUrlDecoder().decode(parts[1])).hasSize(32);
        Double expires = template.opsForZSet().score("cg:{" + gate + "}:active", entry.queueToken())
                .block(Duration.ofSeconds(5));
        assertThat(session.expiresAtMillis()).isEqualTo(expires.longValue());
        queue.close(gate).block(Duration.ofSeconds(5));
        assertThat(queue.issueSession(entry.queueToken(), user).block(Duration.ofSeconds(5))).isEqualTo(session);
        template.opsForZSet().add("cg:{" + gate + "}:active", entry.queueToken(), 0)
                .block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.issueSession(entry.queueToken(), user).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
    }

    @Test
    void concurrentSessionIssuanceReusesOneCredentialAndExpiry() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
        queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
        queue.redeem(entry.queueToken(), user, 60).block(Duration.ofSeconds(5));
        RedisQueue second = new RedisQueue(template);
        var sessions = Flux.range(0, 100).flatMap(index -> (index % 2 == 0 ? queue : second)
                .issueSession(entry.queueToken(), user), 16).collectList().block(Duration.ofSeconds(10));
        assertThat(sessions).hasSize(100);
        assertThat(sessions.stream().map(QueueSession::sessionToken).distinct().count()).isEqualTo(1);
        assertThat(sessions.stream().map(QueueSession::expiresAtMillis).distinct().count()).isEqualTo(1);
        String prefix = "cg:{" + gate + "}:";
        assertThat(template.opsForHash().size(prefix + "sessions-by-entry").block(Duration.ofSeconds(5)))
                .isEqualTo(1);
        assertThat(template.opsForHash().size(prefix + "session-entries").block(Duration.ofSeconds(5)))
                .isEqualTo(1);
    }

    @Test
    void admissionReclaimsExpiredActiveSeatForNextWaitingUser() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        String prefix = "cg:{" + gate + "}:";
        QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
        queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
        queue.redeem(first.queueToken(), user, 60).block(Duration.ofSeconds(5));
        QueueEntry next = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        template.opsForZSet().add(prefix + "active", first.queueToken(), 0).block(Duration.ofSeconds(5));
        template.opsForHash().put(prefix + "admission-window", "second", "0").block(Duration.ofSeconds(5));
        assertThat(queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5)))
                .containsExactly(next.queueToken());
        assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("EXPIRED");
        assertThat(template.opsForZSet().size(prefix + "active").block(Duration.ofSeconds(5))).isZero();
        assertThat(template.opsForZSet().size(prefix + "reservations").block(Duration.ofSeconds(5)))
                .isEqualTo(1);
    }

    @Test
    void activeExpiryIsCheckedOnOwnerStatusAndRejoin() {
        for (boolean readFirst : new boolean[]{false, true}) {
            UUID gate = UUID.randomUUID();
            UUID user = UUID.randomUUID();
            String prefix = "cg:{" + gate + "}:";
            QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
            queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
            queue.redeem(first.queueToken(), user, 60).block(Duration.ofSeconds(5));
            assertThat(queue.join(gate, user).block(Duration.ofSeconds(5)).status()).isEqualTo("ACTIVE");
            queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
            template.opsForZSet().add(prefix + "active", first.queueToken(), 0).block(Duration.ofSeconds(5));
            assertThatThrownBy(() -> queue.status(first.queueToken(), UUID.randomUUID())
                    .block(Duration.ofSeconds(5))).isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
            assertThat(template.opsForHash().get(prefix + "statuses", first.queueToken())
                    .block(Duration.ofSeconds(5))).isEqualTo("ACTIVE");
            if (readFirst) {
                assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                        .isEqualTo("EXPIRED");
            }
            QueueEntry rejoined = queue.join(gate, user).block(Duration.ofSeconds(5));
            assertThat(rejoined.queueToken()).isNotEqualTo(first.queueToken());
            assertThat(rejoined.status()).isEqualTo("WAITING");
            assertThat(rejoined.position()).isEqualTo(2);
            assertThat(template.opsForZSet().size(prefix + "active").block(Duration.ofSeconds(5))).isZero();
            assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                    .isEqualTo("EXPIRED");
        }
    }

    @Test
    void redeemsAdmissionOnceWithoutExtendingActiveExpiry() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
        queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.redeem(entry.queueToken(), UUID.randomUUID(), 60)
                .block(Duration.ofSeconds(5))).isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
        QueueEntry active = queue.redeem(entry.queueToken(), user, 60).block(Duration.ofSeconds(5));
        assertThat(active.status()).isEqualTo("ACTIVE");
        assertThat(active.position()).isZero();
        String prefix = "cg:{" + gate + "}:";
        assertThat(template.opsForZSet().size(prefix + "reservations").block(Duration.ofSeconds(5))).isZero();
        assertThat(template.opsForZSet().size(prefix + "active").block(Duration.ofSeconds(5))).isEqualTo(1);
        Double expires = template.opsForZSet().score(prefix + "active", entry.queueToken())
                .block(Duration.ofSeconds(5));
        queue.close(gate).block(Duration.ofSeconds(5));
        assertThat(queue.redeem(entry.queueToken(), user, 300).block(Duration.ofSeconds(5)))
                .isEqualTo(active);
        assertThat(template.opsForZSet().score(prefix + "active", entry.queueToken())
                .block(Duration.ofSeconds(5))).isEqualTo(expires);
        template.opsForZSet().add(prefix + "active", entry.queueToken(), 0).block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.redeem(entry.queueToken(), user, 60).block(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(409));
        assertThat(template.opsForZSet().size(prefix + "active").block(Duration.ofSeconds(5))).isZero();
    }

    @Test
    void rejectsExpiredOrClosedAdmissionRedemption() {
        for (boolean closed : new boolean[]{false, true}) {
            UUID gate = UUID.randomUUID();
            UUID user = UUID.randomUUID();
            QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
            queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
            if (closed) {
                queue.close(gate).block(Duration.ofSeconds(5));
            } else {
                template.opsForZSet().add("cg:{" + gate + "}:reservations", entry.queueToken(), 0)
                        .block(Duration.ofSeconds(5));
            }
            assertThatThrownBy(() -> queue.redeem(entry.queueToken(), user, 60).block(Duration.ofSeconds(5)))
                    .isInstanceOfSatisfying(ResponseStatusException.class,
                            error -> assertThat(error.getStatusCode().value()).isEqualTo(closed ? 410 : 409));
            assertThat(queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5)).status())
                    .isEqualTo(closed ? "CANCELLED" : "EXPIRED");
            assertThat(template.opsForZSet().size("cg:{" + gate + "}:active").block(Duration.ofSeconds(5)))
                    .isZero();
        }
    }

    @Test
    void onlyOwnerAccessExpiresAdmissionWithoutRunner() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
        queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
        String prefix = "cg:{" + gate + "}:";
        template.opsForZSet().add(prefix + "reservations", entry.queueToken(), 0).block(Duration.ofSeconds(5));
        assertThatThrownBy(() -> queue.status(entry.queueToken(), UUID.randomUUID())
                .block(Duration.ofSeconds(5))).isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(404));
        assertThat(template.opsForHash().get(prefix + "statuses", entry.queueToken())
                .block(Duration.ofSeconds(5))).isEqualTo("ADMITTED");
        QueueEntry expired = queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5));
        assertThat(expired.status()).isEqualTo("EXPIRED");
        assertThat(expired.position()).isZero();
        assertThat(template.opsForZSet().size(prefix + "reservations").block(Duration.ofSeconds(5))).isZero();
    }

    @Test
    void expiredAdmissionRejoinsAtTailAndCloseOverridesExpiry() {
        for (boolean closed : new boolean[]{false, true}) {
            UUID gate = UUID.randomUUID();
            UUID user = UUID.randomUUID();
            QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
            queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5));
            QueueEntry waiting = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
            template.opsForZSet().add("cg:{" + gate + "}:reservations", first.queueToken(), 0)
                    .block(Duration.ofSeconds(5));
            if (closed) {
                queue.close(gate).block(Duration.ofSeconds(5));
                assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                        .isEqualTo("CANCELLED");
            } else {
                QueueEntry rejoined = queue.join(gate, user).block(Duration.ofSeconds(5));
                assertThat(rejoined.queueToken()).isNotEqualTo(first.queueToken());
                assertThat(rejoined.status()).isEqualTo("WAITING");
                assertThat(rejoined.position()).isEqualTo(2);
                assertThat(rejoined.queueSize()).isEqualTo(2);
                assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                        .isEqualTo("EXPIRED");
                assertThat(queue.join(gate, user).block(Duration.ofSeconds(5)).queueToken())
                        .isEqualTo(rejoined.queueToken());
                assertThat(waiting.position()).isEqualTo(1);
            }
        }
    }

    @Test
    void sharesAdmissionBudgetAcrossCallsWithinRedisSecond() throws java.io.IOException {
        UUID gate = UUID.randomUUID();
        queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        String prefix = "cg:{" + gate + "}:";
        var resource = new org.springframework.core.io.ClassPathResource("redis/admit.lua");
        String source = resource.getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
        String setup = "redis.call('HSET', KEYS[8], 'second', redis.call('TIME')[1], 'count', ARGV[1])\n";
        var script = new org.springframework.data.redis.core.script.DefaultRedisScript<java.util.List>(
                setup + source, java.util.List.class);
        var keys = java.util.List.of(prefix + "waiting", prefix + "identities", prefix + "statuses",
                prefix + "owners", prefix + "sequence", prefix + "closed", prefix + "reservations",
                prefix + "admission-window", prefix + "active");
        var result = template.execute(script, keys, java.util.List.of("2", "10", "60"))
                .single().block(Duration.ofSeconds(5));
        assertThat(result).isEmpty();
        assertThat(template.opsForZSet().size(prefix + "waiting").block(Duration.ofSeconds(5)))
                .isEqualTo(1);
        assertThat(template.opsForHash().get(prefix + "admission-window", "count")
                .block(Duration.ofSeconds(5))).isEqualTo("2");
    }

    @Test
    void expiresAdmittedReservationsAndCountsActiveSeats() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        String prefix = "cg:{" + gate + "}:";
        QueueEntry first = queue.join(gate, user).block(Duration.ofSeconds(5));
        QueueEntry next = queue.join(gate, UUID.randomUUID()).block(Duration.ofSeconds(5));
        assertThat(queue.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5)))
                .containsExactly(first.queueToken());
        template.opsForZSet().add(prefix + "reservations", first.queueToken(), 0).block(Duration.ofSeconds(5));
        template.opsForHash().put(prefix + "admission-window", "second", "0").block(Duration.ofSeconds(5));
        template.opsForZSet().add(prefix + "active", "active-session", Double.MAX_VALUE)
                .block(Duration.ofSeconds(5));
        RedisQueue second = new RedisQueue(template);
        assertThat(second.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5))).isEmpty();
        assertThat(queue.status(first.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("EXPIRED");
        template.opsForZSet().remove(prefix + "active", "active-session").block(Duration.ofSeconds(5));
        assertThat(second.admit(gate, 1, 1, 60).block(Duration.ofSeconds(5)))
                .containsExactly(next.queueToken());
        assertThat(template.opsForZSet().size(prefix + "reservations").block(Duration.ofSeconds(5)))
                .isEqualTo(1);
    }

    @Test
    void admitsFifoEntriesWithoutExceedingReservedCapacity() {
        UUID gate = UUID.randomUUID();
        UUID firstUser = UUID.randomUUID();
        UUID secondUser = UUID.randomUUID();
        UUID thirdUser = UUID.randomUUID();
        QueueEntry first = queue.join(gate, firstUser).block(Duration.ofSeconds(5));
        QueueEntry second = queue.join(gate, secondUser).block(Duration.ofSeconds(5));
        QueueEntry third = queue.join(gate, thirdUser).block(Duration.ofSeconds(5));

        var admitted = queue.admit(gate, 2, 2, 60).block(Duration.ofSeconds(5));
        assertThat(admitted).containsExactly(first.queueToken(), second.queueToken());
        for (var owner : java.util.Map.of(first.queueToken(), firstUser, second.queueToken(), secondUser)
                .entrySet()) {
            QueueEntry status = queue.status(owner.getKey(), owner.getValue()).block(Duration.ofSeconds(5));
            assertThat(status.status()).isEqualTo("ADMITTED");
            assertThat(status.position()).isZero();
            assertThat(status.queueSize()).isEqualTo(1);
        }
        QueueEntry waiting = queue.status(third.queueToken(), thirdUser).block(Duration.ofSeconds(5));
        assertThat(waiting.status()).isEqualTo("WAITING");
        assertThat(waiting.position()).isEqualTo(1);
        assertThat(queue.admit(gate, 2, 2, 60).block(Duration.ofSeconds(5))).isEmpty();
        assertThat(template.opsForZSet().size("cg:{" + gate + "}:reservations")
                .block(Duration.ofSeconds(5))).isEqualTo(2);
        assertThat(queue.join(gate, firstUser).block(Duration.ofSeconds(5)).queueToken())
                .isEqualTo(first.queueToken());
    }

    @Test
    void doesNotAdmitClosedQueuesOrInvalidLimits() {
        UUID gate = UUID.randomUUID();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(gate, user).block(Duration.ofSeconds(5));
        queue.close(gate).block(Duration.ofSeconds(5));
        assertThat(queue.admit(gate, 5, 10, 60).block(Duration.ofSeconds(5))).isEmpty();
        assertThat(queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("CANCELLED");
        for (int[] limits : new int[][]{{0, 10, 60}, {1, 0, 60}, {1, 10, 0}, {-1, 10, 60}}) {
            assertThatThrownBy(() -> queue.admit(gate, limits[0], limits[1], limits[2])
                    .block(Duration.ofSeconds(5))).isInstanceOf(IllegalArgumentException.class);
        }
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
