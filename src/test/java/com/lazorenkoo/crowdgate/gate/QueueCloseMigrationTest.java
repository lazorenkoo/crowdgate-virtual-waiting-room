package com.lazorenkoo.crowdgate.gate;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class QueueCloseMigrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");

    @Test
    void migratesExistingGatesWithoutLosingData() throws Exception {
        Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .target("1").load().migrate();
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword())) {
            String insert = """
                    INSERT INTO gates (id, name, status, admission_rate, max_active_users,
                        session_ttl_seconds, token_ttl_seconds, created_at, updated_at, version)
                    VALUES (?, ?, ?, 10, 100, 300, 120, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 7)
                    """;
            try (var statement = connection.prepareStatement(insert)) {
                for (String status : new String[]{"CLOSED", "OPEN"}) {
                    statement.setObject(1, UUID.randomUUID());
                    statement.setString(2, "Existing " + status);
                    statement.setString(3, status);
                    statement.executeUpdate();
                }
            }
            Flyway upgraded = Flyway.configure().dataSource(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword()).load();
            assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(1);
            assertThat(upgraded.migrate().migrationsExecuted).isZero();
            try (var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT name, status, version, queue_close_pending FROM gates ORDER BY status")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("name")).isEqualTo("Existing CLOSED");
                assertThat(rows.getBoolean("queue_close_pending")).isTrue();
                assertThat(rows.getLong("version")).isEqualTo(7);
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("name")).isEqualTo("Existing OPEN");
                assertThat(rows.getBoolean("queue_close_pending")).isFalse();
                assertThat(rows.getLong("version")).isEqualTo(7);
                assertThat(rows.next()).isFalse();
            }
            upgraded.validate();
        }
    }
}
