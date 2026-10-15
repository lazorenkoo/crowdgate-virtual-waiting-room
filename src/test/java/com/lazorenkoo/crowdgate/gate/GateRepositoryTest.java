package com.lazorenkoo.crowdgate.gate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import com.lazorenkoo.crowdgate.queue.QueueEntry;
import com.lazorenkoo.crowdgate.queue.RedisQueue;
import org.springframework.data.domain.PageRequest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.UUID;
import java.util.Map;
import java.time.Instant;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"crowdgate.schedule.enabled=false", "crowdgate.admin.key=test-admin-key"})
@Testcontainers
class GateRepositoryTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7.4").withExposedPorts(6379);

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    GateRepository repository;

    @Autowired
    WebTestClient client;

    @Autowired
    RedisQueue queue;

    @BeforeEach
    void authenticateClient() {
        client = client.mutate().defaultHeader("X-Admin-Key", "test-admin-key").build();
    }

    @Test
    void admissionRunnerAdmitsOnlyOpenGatesWithAvailableCapacity() {
        UUID user = UUID.randomUUID();
        var entries = new java.util.EnumMap<GateStatus, QueueEntry>(GateStatus.class);
        for (GateStatus status : GateStatus.values()) {
            Gate gate = new Gate("Admission " + status, 10, 1, 300, 120);
            if (status == GateStatus.SCHEDULED) {
                gate.setSchedule(Instant.now().plusSeconds(300), null);
            } else if (status == GateStatus.PAUSED) {
                gate.changeStatus(GateStatus.OPEN);
                gate.changeStatus(status);
            } else if (status != GateStatus.DRAFT) {
                gate.changeStatus(status);
            }
            UUID id = repository.saveAndFlush(gate).getId();
            entries.put(status, queue.join(id, user).block(Duration.ofSeconds(5)));
            if (status == GateStatus.OPEN) {
                queue.join(id, UUID.randomUUID()).block(Duration.ofSeconds(5));
            }
        }
        new GateAdmissionRunner(repository, queue).run();
        for (var entry : entries.entrySet()) {
            QueueEntry current = queue.status(entry.getValue().queueToken(), user).block(Duration.ofSeconds(5));
            assertThat(current.status()).isEqualTo(entry.getKey() == GateStatus.OPEN ? "ADMITTED" : "WAITING");
            assertThat(current.position()).isEqualTo(entry.getKey() == GateStatus.OPEN ? 0 : 1);
            if (entry.getKey() == GateStatus.OPEN) {
                assertThat(current.queueSize()).isEqualTo(1);
            }
        }
    }

    @Test
    void admissionQueryUsesUuidCursorAndExcludesExpiredGates() {
        Instant now = Instant.now();
        Gate expired = new Gate("Expired admission", 10, 10, 300, 120);
        expired.setSchedule(now.minusSeconds(20), now.minusSeconds(10));
        expired.changeStatus(GateStatus.OPEN);
        UUID expiredId = repository.saveAndFlush(expired).getId();
        Gate open = new Gate("Cursor admission", 10, 10, 300, 120);
        open.changeStatus(GateStatus.OPEN);
        UUID openId = repository.saveAndFlush(open).getId();
        var all = repository.findAdmissionGates(now, null, PageRequest.of(0, 100));
        assertThat(all).extracting(Gate::getId).contains(openId).doesNotContain(expiredId);
        var first = repository.findAdmissionGates(now, null, PageRequest.of(0, 1));
        assertThat(first).hasSize(1);
        UUID cursor = first.getFirst().getId();
        var remaining = repository.findAdmissionGates(now, cursor, PageRequest.of(0, 100));
        assertThat(remaining).extracting(Gate::getId)
                .containsExactlyElementsOf(all.stream().skip(1).map(Gate::getId).toList());
    }

    @Test
    void concurrentCloseSynchronizersSafelyAcknowledgeSameGate() throws Exception {
        Gate gate = new Gate("Parallel close recovery", 10, 100, 300, 120);
        gate.changeStatus(GateStatus.OPEN);
        UUID id = repository.saveAndFlush(gate).getId();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(id, user).block(Duration.ofSeconds(5));
        gate = repository.findById(id).orElseThrow();
        gate.changeStatus(GateStatus.CLOSED);
        long version = repository.saveAndFlush(gate).getVersion();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                ready.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Close start timed out");
                }
                new GateQueueCloseSynchronizer(repository, queue).close(id).block(Duration.ofSeconds(5));
                return null;
            });
            var second = executor.submit(() -> {
                ready.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Close start timed out");
                }
                new GateQueueCloseSynchronizer(repository, queue).close(id).block(Duration.ofSeconds(5));
                return null;
            });
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
        Gate completed = repository.findById(id).orElseThrow();
        assertThat(completed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(completed.getVersion()).isEqualTo(version);
        assertThat(completed.isQueueClosePending()).isFalse();
        assertThat(repository.acknowledgeQueueClose(id)).isZero();
        QueueEntry cancelled = queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5));
        assertThat(cancelled.status()).isEqualTo("CANCELLED");
        assertThat(cancelled.queueSize()).isZero();
    }

    @Test
    void recoversScheduledCloseAfterOnlyDatabaseUpdateCompleted() {
        Instant closing = Instant.parse("2040-01-01T10:00:00Z");
        Gate gate = new Gate("Recover scheduled close", 10, 100, 300, 120);
        gate.setSchedule(closing.minusSeconds(120), closing);
        gate.changeStatus(GateStatus.OPEN);
        UUID id = repository.saveAndFlush(gate).getId();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(id, user).block(Duration.ofSeconds(5));
        repository.closeDue(closing);
        Gate pending = repository.findById(id).orElseThrow();
        assertThat(pending.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(pending.isQueueClosePending()).isTrue();
        assertThat(queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("WAITING");

        new GateQueueCloseSynchronizer(repository, queue).run();

        Gate completed = repository.findById(id).orElseThrow();
        assertThat(completed.isQueueClosePending()).isFalse();
        assertThat(completed.getVersion()).isEqualTo(pending.getVersion());
        assertThat(queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("CANCELLED");
        client.post().uri("/api/v1/gates/{id}/join", id).exchange()
                .expectStatus().isEqualTo(410).expectBody().returnResult();
        new GateQueueCloseSynchronizer(repository, queue).run();
        assertThat(repository.findById(id).orElseThrow().getVersion()).isEqualTo(pending.getVersion());
    }

    @Test
    void replaysRedisCloseWhenProcessStoppedBeforeDatabaseAcknowledgment() {
        Gate gate = new Gate("Recover close acknowledgment", 10, 100, 300, 120);
        gate.changeStatus(GateStatus.OPEN);
        UUID id = repository.saveAndFlush(gate).getId();
        UUID user = UUID.randomUUID();
        QueueEntry entry = queue.join(id, user).block(Duration.ofSeconds(5));
        gate = repository.findById(id).orElseThrow();
        gate.changeStatus(GateStatus.CLOSED);
        repository.saveAndFlush(gate);
        queue.close(id).block(Duration.ofSeconds(5));
        assertThat(repository.findById(id).orElseThrow().isQueueClosePending()).isTrue();

        new GateQueueCloseSynchronizer(repository, queue).run();

        assertThat(repository.findById(id).orElseThrow().isQueueClosePending()).isFalse();
        assertThat(queue.status(entry.queueToken(), user).block(Duration.ofSeconds(5)).status())
                .isEqualTo("CANCELLED");
    }

    @Test
    void persistsPendingCloseAndAcknowledgesWithoutChangingPublicVersion() {
        Gate gate = repository.saveAndFlush(new Gate("Pending close", 10, 100, 300, 120));
        assertThat(gate.isQueueClosePending()).isFalse();
        gate.changeStatus(GateStatus.CLOSED);
        gate = repository.saveAndFlush(gate);
        Gate pending = repository.findById(gate.getId()).orElseThrow();
        assertThat(pending.isQueueClosePending()).isTrue();
        assertThat(repository.findPendingQueueCloses(PageRequest.of(0, 1000)))
                .extracting(Gate::getId).contains(gate.getId());
        assertThat(repository.findPendingQueueCloses(PageRequest.of(0, 1))).hasSize(1);
        assertThat(repository.acknowledgeQueueClose(gate.getId())).isEqualTo(1);
        assertThat(repository.acknowledgeQueueClose(gate.getId())).isZero();
        Gate completed = repository.findById(gate.getId()).orElseThrow();
        assertThat(completed.isQueueClosePending()).isFalse();
        assertThat(completed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(completed.getVersion()).isEqualTo(pending.getVersion());
        client.get().uri("/api/v1/admin/gates/{id}", gate.getId()).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.queueClosePending").doesNotExist();
    }

    @Test
    void scheduledClosePersistsPendingWorkInSameDatabaseUpdate() {
        Instant now = Instant.parse("2035-01-01T10:00:00Z");
        Gate gate = new Gate("Scheduled pending close", 10, 100, 300, 120);
        gate.setSchedule(now.minusSeconds(120), now.minusSeconds(60));
        UUID id = repository.saveAndFlush(gate).getId();
        repository.closeDue(now);
        Gate closed = repository.findById(id).orElseThrow();
        assertThat(closed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(closed.isQueueClosePending()).isTrue();
        assertThat(closed.getVersion()).isEqualTo(1);
        repository.closeDue(now);
        assertThat(repository.findById(id).orElseThrow().getVersion()).isEqualTo(1);
    }

    @Test
    void manualCloseCancelsPublicQueueBeforeReturning() {
        Gate gate = new Gate("Manual queue close", 10, 100, 300, 120);
        gate.changeStatus(GateStatus.OPEN);
        UUID id = repository.saveAndFlush(gate).getId();
        String user = UUID.randomUUID().toString();
        QueueEntry entry = client.post().uri("/api/v1/gates/{id}/join", id)
                .cookie("cg_user_id", user).exchange().expectStatus().isOk()
                .expectBody(QueueEntry.class).returnResult().getResponseBody();
        client.post().uri("/api/v1/admin/gates/{id}/close", id).exchange()
                .expectStatus().isOk().expectBody().returnResult();
        client.get().uri("/api/v1/queue/{token}", entry.queueToken()).cookie("cg_user_id", user)
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("CANCELLED")
                .jsonPath("$.position").isEqualTo(0)
                .jsonPath("$.queueSize").isEqualTo(0);
        client.post().uri("/api/v1/admin/gates/{id}/close", id).exchange()
                .expectStatus().isOk().expectBody().returnResult();
        client.post().uri("/api/v1/gates/{id}/join", id).exchange()
                .expectStatus().isEqualTo(410).expectBody().returnResult();
    }

    @Test
    void appliesGatePolicyToPublicJoin() {
        WebTestClient publicClient = client.mutate()
                .defaultHeaders(headers -> headers.remove("X-Admin-Key")).build();
        Gate gate = repository.saveAndFlush(new Gate("Queue policy", 10, 100, 300, 120));
        String path = "/api/v1/gates/" + gate.getId() + "/join";
        publicClient.post().uri(path).exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        gate.setSchedule(Instant.now().plusSeconds(3600), null);
        gate = repository.saveAndFlush(gate);
        publicClient.post().uri(path).exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        gate.changeStatus(GateStatus.OPEN);
        gate.changeStatus(GateStatus.PAUSED);
        gate = repository.saveAndFlush(gate);
        publicClient.post().uri(path).exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("WAITING");
        gate.changeStatus(GateStatus.CLOSED);
        repository.saveAndFlush(gate);
        publicClient.post().uri(path).exchange().expectStatus().isEqualTo(410).expectBody().returnResult();
        Gate expired = new Gate("Expired queue", 10, 100, 300, 120);
        expired.setSchedule(Instant.EPOCH, Instant.EPOCH.plusSeconds(60));
        expired.changeStatus(GateStatus.OPEN);
        UUID expiredId = repository.saveAndFlush(expired).getId();
        publicClient.post().uri("/api/v1/gates/{id}/join", expiredId)
                .exchange().expectStatus().isEqualTo(410).expectBody().returnResult();
        publicClient.post().uri("/api/v1/gates/{id}/join", UUID.randomUUID())
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
    }

    @Test
    void joinsQueueWithCookieAndReadsOwnedStatus() {
        Gate gate = new Gate("Queue API", 10, 100, 300, 120);
        gate.changeStatus(GateStatus.OPEN);
        UUID id = repository.saveAndFlush(gate).getId();
        WebTestClient publicClient = client.mutate()
                .defaultHeaders(headers -> headers.remove("X-Admin-Key")).build();
        String path = "/api/v1/gates/" + id + "/join";
        var response = publicClient.post().uri(path).exchange().expectStatus().isOk()
                .expectCookie().httpOnly("cg_user_id", true)
                .expectCookie().sameSite("cg_user_id", "Lax")
                .expectBody(QueueEntry.class).returnResult();
        QueueEntry first = response.getResponseBody();
        String cookie = response.getResponseCookies().getFirst("cg_user_id").getValue();
        assertThat(first.status()).isEqualTo("WAITING");
        assertThat(first.position()).isEqualTo(1);
        publicClient.post().uri(path).cookie("cg_user_id", cookie).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.queueToken").isEqualTo(first.queueToken())
                .jsonPath("$.queueSize").isEqualTo(1);
        publicClient.post().uri(path).exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.position").isEqualTo(2);
        publicClient.get().uri("/api/v1/queue/{token}", first.queueToken())
                .cookie("cg_user_id", cookie).exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.position").isEqualTo(1)
                .jsonPath("$.queueSize").isEqualTo(2);
        publicClient.get().uri("/api/v1/queue/{token}", first.queueToken())
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        publicClient.get().uri("/api/v1/queue/{token}", first.queueToken())
                .cookie("cg_user_id", UUID.randomUUID().toString())
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
    }

    @Test
    void parallelSchedulersApplyEachTransitionOnce() throws Exception {
        Instant now = Instant.parse("2032-01-01T10:00:00Z");
        Gate ready = new Gate("Parallel opening", 10, 100, 300, 120);
        ready.setSchedule(now, now.plusSeconds(60));
        UUID readyId = repository.saveAndFlush(ready).getId();
        Gate expired = new Gate("Parallel closing", 10, 100, 300, 120);
        expired.setSchedule(now.minusSeconds(120), now.minusSeconds(60));
        UUID expiredId = repository.saveAndFlush(expired).getId();
        CountDownLatch waiting = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                waiting.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Scheduler start timed out");
                }
                new GateScheduleRunner(repository).tick(now);
                return null;
            });
            var second = executor.submit(() -> {
                waiting.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Scheduler start timed out");
                }
                new GateScheduleRunner(repository).tick(now);
                return null;
            });
            assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
        Gate opened = repository.findById(readyId).orElseThrow();
        Gate closed = repository.findById(expiredId).orElseThrow();
        assertThat(opened.getStatus()).isEqualTo(GateStatus.OPEN);
        assertThat(opened.getVersion()).isEqualTo(1);
        assertThat(closed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(closed.getVersion()).isEqualTo(1);
        opened.changeStatus(GateStatus.PAUSED);
        repository.saveAndFlush(opened);
        new GateScheduleRunner(repository).tick(now);
        assertThat(repository.findById(readyId).orElseThrow().getStatus()).isEqualTo(GateStatus.PAUSED);
        assertThat(repository.findById(expiredId).orElseThrow().getVersion()).isEqualTo(1);
    }

    @Test
    void requiresAdminKeyBeforeReadingOrWritingGates() {
        WebTestClient anonymous = client.mutate()
                .defaultHeaders(headers -> headers.remove("X-Admin-Key")).build();
        long count = repository.count();
        anonymous.get().uri("/api/v1/admin/gates").exchange()
                .expectStatus().isUnauthorized().expectBody()
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.message").isEqualTo("Admin key is required")
                .jsonPath("$.path").isEqualTo("/api/v1/admin/gates");
        anonymous.post().uri("/api/v1/admin/gates")
                .bodyValue(new CreateGateRequest("Unauthorized", 10, 100, 300, 120))
                .exchange().expectStatus().isUnauthorized().expectBody().returnResult();
        anonymous.get().uri("/api/v1/admin/gates").header("X-Admin-Key", "wrong-key")
                .exchange().expectStatus().isUnauthorized().expectBody().returnResult();
        assertThat(repository.count()).isEqualTo(count);
        client.get().uri("/api/v1/admin/gates").exchange().expectStatus().isOk().expectBody().returnResult();
    }

    @Test
    void appliesAdminAccessToUnknownAdminRoutesOnly() {
        WebTestClient anonymous = client.mutate()
                .defaultHeaders(headers -> headers.remove("X-Admin-Key")).build();
        anonymous.get().uri("/api/v1/admin").exchange().expectStatus().isUnauthorized().expectBody().returnResult();
        anonymous.get().uri("/api/v1/admin/unknown").exchange().expectStatus().isUnauthorized().expectBody().returnResult();
        anonymous.get().uri("/api/v1/admin-other").exchange().expectStatus().isNotFound().expectBody().returnResult();
    }

    @Test
    void rejectsManualOpeningAfterClosingTimeBeforeSchedulerRuns() {
        for (GateStatus status : new GateStatus[]{GateStatus.SCHEDULED, GateStatus.PAUSED}) {
            Gate gate = new Gate("Expired " + status, 10, 100, 300, 120);
            gate.setSchedule(Instant.EPOCH, Instant.EPOCH.plusSeconds(60));
            if (status == GateStatus.PAUSED) {
                gate.changeStatus(GateStatus.OPEN);
                gate.changeStatus(GateStatus.PAUSED);
            }
            UUID id = repository.saveAndFlush(gate).getId();
            client.post().uri("/api/v1/admin/gates/{id}/open", id).exchange()
                    .expectStatus().isEqualTo(409).expectBody()
                    .jsonPath("$.message").isEqualTo("Gate closing time has passed");
            Gate unchanged = repository.findById(id).orElseThrow();
            assertThat(unchanged.getStatus()).isEqualTo(status);
            assertThat(unchanged.getVersion()).isZero();
        }
    }

    @Test
    void returnsConsistentApiErrors() {
        String path = "/api/v1/admin/gates/" + UUID.randomUUID();
        client.get().uri(path).exchange().expectStatus().isNotFound().expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Gate not found")
                .jsonPath("$.path").isEqualTo(path);
        client.get().uri("/unknown?secret=value").exchange().expectStatus().isNotFound()
                .expectBody().jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.path").isEqualTo("/unknown");
        client.delete().uri("/api/v1/admin/gates").exchange().expectStatus().isEqualTo(405)
                .expectHeader().exists("Allow").expectBody()
                .jsonPath("$.status").isEqualTo(405);
        client.post().uri("/api/v1/admin/gates")
                .header("Content-Type", "application/json").bodyValue("{broken")
                .exchange().expectStatus().isBadRequest().expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.message").isEqualTo("Bad Request");
        Gate gate = repository.saveAndFlush(new Gate("Error conflict", 10, 100, 300, 120));
        client.post().uri("/api/v1/admin/gates/{id}/pause", gate.getId())
                .exchange().expectStatus().isEqualTo(409).expectBody()
                .jsonPath("$.status").isEqualTo(409)
                .jsonPath("$.message").isEqualTo("Cannot change gate from DRAFT to PAUSED");
    }

    @Test
    void runsScheduleAtBoundariesWithoutRepeatingTransitions() {
        Instant opening = Instant.parse("2030-01-01T10:00:00Z");
        Instant closing = opening.plusSeconds(60);
        Gate gate = new Gate("Timed", 10, 100, 300, 120);
        gate.setSchedule(opening, closing);
        UUID id = repository.saveAndFlush(gate).getId();
        GateScheduleRunner runner = new GateScheduleRunner(repository);

        runner.tick(opening.minusSeconds(1));
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(GateStatus.SCHEDULED);
        runner.tick(opening);
        Gate opened = repository.findById(id).orElseThrow();
        assertThat(opened.getStatus()).isEqualTo(GateStatus.OPEN);
        assertThat(opened.getVersion()).isEqualTo(1);
        assertThat(opened.getUpdatedAt()).isEqualTo(opening);
        runner.tick(opening);
        assertThat(repository.findById(id).orElseThrow().getVersion()).isEqualTo(1);
        opened.changeStatus(GateStatus.PAUSED);
        repository.saveAndFlush(opened);
        runner.tick(closing.minusSeconds(1));
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(GateStatus.PAUSED);
        runner.tick(closing);
        Gate closed = repository.findById(id).orElseThrow();
        assertThat(closed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(closed.getVersion()).isEqualTo(3);
        runner.tick(closing.plusSeconds(60));
        assertThat(repository.findById(id).orElseThrow().getVersion()).isEqualTo(3);
    }

    @Test
    void skipsExpiredWindowAndRejectsSnapshotFromBeforeScheduledOpening() {
        Instant now = Instant.parse("2031-01-01T10:00:00Z");
        Gate missed = new Gate("Missed", 10, 100, 300, 120);
        missed.setSchedule(now.minusSeconds(120), now.minusSeconds(60));
        UUID missedId = repository.saveAndFlush(missed).getId();
        Gate ready = new Gate("Ready", 10, 100, 300, 120);
        ready.setSchedule(now, null);
        UUID readyId = repository.saveAndFlush(ready).getId();
        Gate snapshot = repository.findById(readyId).orElseThrow();

        new GateScheduleRunner(repository).tick(now);
        assertThat(repository.findById(missedId).orElseThrow().getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(repository.findById(missedId).orElseThrow().getVersion()).isEqualTo(1);
        assertThat(repository.findById(readyId).orElseThrow().getStatus()).isEqualTo(GateStatus.OPEN);
        snapshot.updateSettings(new CreateGateRequest("Outdated", 20, 100, 300, 120));
        assertThatThrownBy(() -> repository.saveAndFlush(snapshot))
                .isInstanceOf(OptimisticLockingFailureException.class);
        assertThat(repository.findById(readyId).orElseThrow().getName()).isEqualTo("Ready");
    }

    @Test
    void configuresAndClearsSchedule() {
        Gate gate = repository.saveAndFlush(new Gate("Scheduled", 10, 100, 300, 120));
        Instant opening = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
        Instant closing = opening.plusSeconds(3600);
        GateScheduleRequest request = new GateScheduleRequest(0L, opening, closing);
        String path = "/api/v1/admin/gates/" + gate.getId() + "/schedule";
        client.put().uri(path).bodyValue(request).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("SCHEDULED")
                .jsonPath("$.version").isEqualTo(1);
        Gate scheduled = repository.findById(gate.getId()).orElseThrow();
        assertThat(scheduled.getOpensAt()).isEqualTo(opening);
        assertThat(scheduled.getClosesAt()).isEqualTo(closing);
        client.put().uri(path).bodyValue(request).exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        client.put().uri(path).bodyValue(new GateScheduleRequest(1L, null, null))
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("DRAFT");
        Gate cleared = repository.findById(gate.getId()).orElseThrow();
        assertThat(cleared.getOpensAt()).isNull();
        assertThat(cleared.getClosesAt()).isNull();
        assertThat(cleared.getVersion()).isEqualTo(2);
    }

    @Test
    void rejectsInvalidSchedulesAndRunningGateChanges() {
        Gate gate = repository.saveAndFlush(new Gate("Schedule validation", 10, 100, 300, 120));
        Instant future = Instant.now().plusSeconds(3600);
        GateScheduleRequest[] invalid = {
                new GateScheduleRequest(null, future, null),
                new GateScheduleRequest(0L, Instant.EPOCH, null),
                new GateScheduleRequest(0L, future, future),
                new GateScheduleRequest(0L, future, future.minusSeconds(1)),
                new GateScheduleRequest(0L, null, Instant.EPOCH)
        };
        String path = "/api/v1/admin/gates/" + gate.getId() + "/schedule";
        for (GateScheduleRequest request : invalid) {
            client.put().uri(path).bodyValue(request).exchange().expectStatus().isBadRequest().expectBody().returnResult();
        }
        assertThat(repository.findById(gate.getId()).orElseThrow().getVersion()).isZero();
        client.put().uri("/api/v1/admin/gates/{id}/schedule", UUID.randomUUID())
                .bodyValue(new GateScheduleRequest(0L, future, null))
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        client.post().uri("/api/v1/admin/gates/{id}/open", gate.getId())
                .exchange().expectStatus().isOk().expectBody().returnResult();
        client.put().uri(path).bodyValue(new GateScheduleRequest(1L, future, null))
                .exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        assertThat(repository.findById(gate.getId()).orElseThrow().getStatus()).isEqualTo(GateStatus.OPEN);
    }

    @Test
    void followsGateLifecycleAndKeepsRepeatedCommandsIdempotent() {
        Gate gate = repository.saveAndFlush(new Gate("Lifecycle", 10, 100, 300, 120));
        String[] commands = {"open", "pause", "open", "close"};
        String[] statuses = {"OPEN", "PAUSED", "OPEN", "CLOSED"};
        for (int index = 0; index < commands.length; index++) {
            String path = "/api/v1/admin/gates/" + gate.getId() + "/" + commands[index];
            client.post().uri(path).exchange().expectStatus().isOk().expectBody()
                    .jsonPath("$.status").isEqualTo(statuses[index])
                    .jsonPath("$.version").isEqualTo(index + 1);
            client.post().uri(path).exchange().expectStatus().isOk().expectBody()
                    .jsonPath("$.status").isEqualTo(statuses[index])
                    .jsonPath("$.version").isEqualTo(index + 1);
        }
        client.post().uri("/api/v1/admin/gates/{id}/open", gate.getId())
                .exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        client.post().uri("/api/v1/admin/gates/{id}/pause", gate.getId())
                .exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        Gate closed = repository.findById(gate.getId()).orElseThrow();
        assertThat(closed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(closed.getVersion()).isEqualTo(4);
    }

    @Test
    void rejectsPauseBeforeOpeningAndMissingGateCommands() {
        Gate gate = repository.saveAndFlush(new Gate("Draft", 10, 100, 300, 120));
        client.post().uri("/api/v1/admin/gates/{id}/pause", gate.getId())
                .exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        Gate draft = repository.findById(gate.getId()).orElseThrow();
        assertThat(draft.getStatus()).isEqualTo(GateStatus.DRAFT);
        assertThat(draft.getVersion()).isZero();
        for (String command : new String[]{"open", "pause", "close"}) {
            client.post().uri("/api/v1/admin/gates/" + UUID.randomUUID() + "/" + command)
                    .exchange().expectStatus().isNotFound().expectBody().returnResult();
        }
    }

    @Test
    void closesDraftWithoutOpeningIt() {
        Gate gate = repository.saveAndFlush(new Gate("Cancelled launch", 10, 100, 300, 120));
        client.post().uri("/api/v1/admin/gates/{id}/close", gate.getId())
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("CLOSED");
        assertThat(repository.findById(gate.getId()).orElseThrow().getStatus())
                .isEqualTo(GateStatus.CLOSED);
    }

    @Test
    void updatesSettingsWithoutOverwritingOtherFields() {
        Gate original = repository.saveAndFlush(new Gate("Original", 10, 100, 300, 120));
        original = repository.findById(original.getId()).orElseThrow();
        UpdateGateRequest request = new UpdateGateRequest(0L, " Renamed ", 25, null, null, null);
        client.patch().uri("/api/v1/admin/gates/{id}", original.getId()).bodyValue(request)
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.name").isEqualTo("Renamed")
                .jsonPath("$.admissionRate").isEqualTo(25)
                .jsonPath("$.maxActiveUsers").isEqualTo(100)
                .jsonPath("$.sessionTtlSeconds").isEqualTo(300)
                .jsonPath("$.tokenTtlSeconds").isEqualTo(120)
                .jsonPath("$.status").isEqualTo("DRAFT")
                .jsonPath("$.version").isEqualTo(1);
        Gate updated = repository.findById(original.getId()).orElseThrow();
        assertThat(updated.getCreatedAt()).isEqualTo(original.getCreatedAt());
        assertThat(updated.getUpdatedAt()).isAfterOrEqualTo(updated.getCreatedAt());

        client.patch().uri("/api/v1/admin/gates/{id}", original.getId())
                .bodyValue(new UpdateGateRequest(0L, "Stale", null, null, null, null))
                .exchange().expectStatus().isEqualTo(409).expectBody().returnResult();
        Gate unchanged = repository.findById(original.getId()).orElseThrow();
        assertThat(unchanged.getName()).isEqualTo("Renamed");
        assertThat(unchanged.getVersion()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidSettingsUpdates() {
        Gate gate = repository.saveAndFlush(new Gate("Valid", 10, 100, 300, 120));
        UpdateGateRequest[] invalid = {
                new UpdateGateRequest(null, "Changed", null, null, null, null),
                new UpdateGateRequest(-1L, null, null, null, null, null),
                new UpdateGateRequest(0L, " ", null, null, null, null),
                new UpdateGateRequest(0L, null, null, 0, null, null),
                new UpdateGateRequest(0L, null, null, null, null, -1)
        };
        for (UpdateGateRequest request : invalid) {
            client.patch().uri("/api/v1/admin/gates/{id}", gate.getId()).bodyValue(request)
                    .exchange().expectStatus().isBadRequest().expectBody().returnResult();
        }
        client.patch().uri("/api/v1/admin/gates/{id}", UUID.randomUUID())
                .bodyValue(new UpdateGateRequest(0L, "Missing", null, null, null, null))
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        Gate unchanged = repository.findById(gate.getId()).orElseThrow();
        assertThat(unchanged.getName()).isEqualTo("Valid");
        assertThat(unchanged.getVersion()).isZero();
    }

    @Test
    void preventsAnOlderCopyFromOverwritingNewSettings() {
        Gate saved = repository.saveAndFlush(new Gate("Original", 10, 100, 300, 120));
        Gate first = repository.findById(saved.getId()).orElseThrow();
        Gate second = repository.findById(saved.getId()).orElseThrow();
        first.updateSettings(new CreateGateRequest("Winner", 20, 100, 300, 120));
        repository.saveAndFlush(first);
        second.updateSettings(new CreateGateRequest("Stale", 30, 100, 300, 120));

        assertThatThrownBy(() -> repository.saveAndFlush(second))
                .isInstanceOf(OptimisticLockingFailureException.class);
        Gate result = repository.findById(saved.getId()).orElseThrow();
        assertThat(result.getName()).isEqualTo("Winner");
        assertThat(result.getAdmissionRate()).isEqualTo(20);
    }

    @Test
    void listsGatesInSeparatePages() {
        repository.deleteAll();
        Gate first = repository.saveAndFlush(new Gate("First", 10, 100, 300, 120));
        Gate second = repository.saveAndFlush(new Gate("Second", 10, 100, 300, 120));
        Gate third = repository.saveAndFlush(new Gate("Third", 10, 100, 300, 120));

        client.get().uri("/api/v1/admin/gates?page=0&size=2").exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.items.length()").isEqualTo(2)
                .jsonPath("$.items[0].id").isEqualTo(first.getId().toString())
                .jsonPath("$.items[1].id").isEqualTo(second.getId().toString())
                .jsonPath("$.page").isEqualTo(0)
                .jsonPath("$.size").isEqualTo(2)
                .jsonPath("$.totalItems").isEqualTo(3)
                .jsonPath("$.totalPages").isEqualTo(2);
        client.get().uri("/api/v1/admin/gates?page=1&size=2").exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.items.length()").isEqualTo(1)
                .jsonPath("$.items[0].id").isEqualTo(third.getId().toString())
                .jsonPath("$.totalItems").isEqualTo(3);
        client.get().uri("/api/v1/admin/gates?page=2&size=2").exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.items.length()").isEqualTo(0)
                .jsonPath("$.totalItems").isEqualTo(3);
        client.get().uri("/api/v1/admin/gates").exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.page").isEqualTo(0)
                .jsonPath("$.size").isEqualTo(20)
                .jsonPath("$.items.length()").isEqualTo(3);
    }

    @Test
    void rejectsInvalidPageParameters() {
        String[] queries = {"page=-1", "size=0", "size=101", "page=invalid"};
        for (String query : queries) {
            client.get().uri("/api/v1/admin/gates?" + query).exchange()
                    .expectStatus().isBadRequest().expectBody().returnResult();
        }
    }

    @Test
    void createsAndReadsGateThroughApi() {
        CreateGateRequest request = new CreateGateRequest(" API launch ", 10, 100, 300, 120);
        Map<?, ?> response = client.post().uri("/api/v1/admin/gates").bodyValue(request)
                .exchange().expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        assertThat(response).isNotNull();
        assertThat(response.get("name")).isEqualTo("API launch");
        assertThat(response.get("status")).isEqualTo("DRAFT");
        UUID id = UUID.fromString((String) response.get("id"));
        assertThat(repository.findById(id)).isPresent();

        client.get().uri("/api/v1/admin/gates/{id}", id).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.id").isEqualTo(id.toString())
                .jsonPath("$.admissionRate").isEqualTo(10)
                .jsonPath("$.sessionTtlSeconds").isEqualTo(300);
    }

    @Test
    void rejectsInvalidApiRequests() {
        long count = repository.count();
        client.post().uri("/api/v1/admin/gates")
                .bodyValue(new CreateGateRequest("Launch", 0, 100, 300, 120))
                .exchange().expectStatus().isBadRequest().expectBody().returnResult();
        client.get().uri("/api/v1/admin/gates/{id}", UUID.randomUUID())
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        client.get().uri("/api/v1/admin/gates/not-a-uuid")
                .exchange().expectStatus().isBadRequest().expectBody().returnResult();
        assertThat(repository.count()).isEqualTo(count);
    }

    @Test
    void savesAndReadsGate() {
        Gate saved = repository.saveAndFlush(new Gate("Launch", 10, 100, 300, 120));
        Gate loaded = repository.findById(saved.getId()).orElseThrow();

        assertThat(loaded.getName()).isEqualTo("Launch");
        assertThat(loaded.getStatus()).isEqualTo(GateStatus.DRAFT);
        assertThat(loaded.getAdmissionRate()).isEqualTo(10);
        assertThat(loaded.getMaxActiveUsers()).isEqualTo(100);
        assertThat(loaded.getSessionTtlSeconds()).isEqualTo(300);
        assertThat(loaded.getTokenTtlSeconds()).isEqualTo(120);
        assertThat(loaded.getCreatedAt()).isNotNull();
        assertThat(loaded.getUpdatedAt()).isEqualTo(loaded.getCreatedAt());
        assertThat(loaded.getVersion()).isZero();
    }

    @Test
    void rejectsInvalidGateWithoutSavingIt() {
        long count = repository.count();

        assertThatThrownBy(() -> repository.saveAndFlush(new Gate("Launch", 0, 100, 300, 120)))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> repository.saveAndFlush(new Gate(" ", 10, 100, 300, 120)))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(repository.count()).isEqualTo(count);
    }
}
