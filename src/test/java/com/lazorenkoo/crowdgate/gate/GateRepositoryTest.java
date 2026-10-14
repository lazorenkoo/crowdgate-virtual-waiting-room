package com.lazorenkoo.crowdgate.gate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.UUID;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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
