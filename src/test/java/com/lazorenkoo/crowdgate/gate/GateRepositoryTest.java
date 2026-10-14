package com.lazorenkoo.crowdgate.gate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
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
