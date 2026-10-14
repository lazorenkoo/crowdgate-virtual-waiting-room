package com.lazorenkoo.crowdgate.gate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.UUID;
import java.util.Map;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "crowdgate.schedule.enabled=false")
@Testcontainers
class GateRepositoryTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    GateRepository repository;

    @Autowired
    WebTestClient client;

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
        client.put().uri(path).bodyValue(request).exchange().expectStatus().isEqualTo(409);
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
            client.put().uri(path).bodyValue(request).exchange().expectStatus().isBadRequest();
        }
        assertThat(repository.findById(gate.getId()).orElseThrow().getVersion()).isZero();
        client.put().uri("/api/v1/admin/gates/{id}/schedule", UUID.randomUUID())
                .bodyValue(new GateScheduleRequest(0L, future, null))
                .exchange().expectStatus().isNotFound();
        client.post().uri("/api/v1/admin/gates/{id}/open", gate.getId())
                .exchange().expectStatus().isOk();
        client.put().uri(path).bodyValue(new GateScheduleRequest(1L, future, null))
                .exchange().expectStatus().isEqualTo(409);
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
                .exchange().expectStatus().isEqualTo(409);
        client.post().uri("/api/v1/admin/gates/{id}/pause", gate.getId())
                .exchange().expectStatus().isEqualTo(409);
        Gate closed = repository.findById(gate.getId()).orElseThrow();
        assertThat(closed.getStatus()).isEqualTo(GateStatus.CLOSED);
        assertThat(closed.getVersion()).isEqualTo(4);
    }

    @Test
    void rejectsPauseBeforeOpeningAndMissingGateCommands() {
        Gate gate = repository.saveAndFlush(new Gate("Draft", 10, 100, 300, 120));
        client.post().uri("/api/v1/admin/gates/{id}/pause", gate.getId())
                .exchange().expectStatus().isEqualTo(409);
        Gate draft = repository.findById(gate.getId()).orElseThrow();
        assertThat(draft.getStatus()).isEqualTo(GateStatus.DRAFT);
        assertThat(draft.getVersion()).isZero();
        for (String command : new String[]{"open", "pause", "close"}) {
            client.post().uri("/api/v1/admin/gates/" + UUID.randomUUID() + "/" + command)
                    .exchange().expectStatus().isNotFound();
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
                .exchange().expectStatus().isEqualTo(409);
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
                    .exchange().expectStatus().isBadRequest();
        }
        client.patch().uri("/api/v1/admin/gates/{id}", UUID.randomUUID())
                .bodyValue(new UpdateGateRequest(0L, "Missing", null, null, null, null))
                .exchange().expectStatus().isNotFound();
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
                    .expectStatus().isBadRequest();
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
                .exchange().expectStatus().isBadRequest();
        client.get().uri("/api/v1/admin/gates/{id}", UUID.randomUUID())
                .exchange().expectStatus().isNotFound();
        client.get().uri("/api/v1/admin/gates/not-a-uuid")
                .exchange().expectStatus().isBadRequest();
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
