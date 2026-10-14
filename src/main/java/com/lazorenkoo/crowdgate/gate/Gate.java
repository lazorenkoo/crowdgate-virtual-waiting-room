package com.lazorenkoo.crowdgate.gate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "gates")
public class Gate {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private GateStatus status = GateStatus.DRAFT;

    private Instant opensAt;
    private Instant closesAt;

    @Column(nullable = false)
    private int admissionRate;

    @Column(nullable = false)
    private int maxActiveUsers;

    @Column(nullable = false)
    private int sessionTtlSeconds;

    @Column(nullable = false)
    private int tokenTtlSeconds;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @Version
    private long version;

    protected Gate() {
    }

    public Gate(String name, int admissionRate, int maxActiveUsers,
                int sessionTtlSeconds, int tokenTtlSeconds) {
        this.name = name;
        this.admissionRate = admissionRate;
        this.maxActiveUsers = maxActiveUsers;
        this.sessionTtlSeconds = sessionTtlSeconds;
        this.tokenTtlSeconds = tokenTtlSeconds;
    }

    @PrePersist
    void setCreationTime() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
    }

    public void updateSettings(CreateGateRequest settings) {
        name = settings.name().trim();
        admissionRate = settings.admissionRate();
        maxActiveUsers = settings.maxActiveUsers();
        sessionTtlSeconds = settings.sessionTtlSeconds();
        tokenTtlSeconds = settings.tokenTtlSeconds();
    }

    public void changeStatus(GateStatus target) {
        if (status == target) {
            return;
        }
        boolean allowed = switch (target) {
            case OPEN -> status == GateStatus.DRAFT || status == GateStatus.SCHEDULED
                    || status == GateStatus.PAUSED;
            case PAUSED -> status == GateStatus.OPEN;
            case CLOSED -> true;
            default -> false;
        };
        if (!allowed) {
            throw new GateTransitionException(status, target);
        }
        status = target;
    }

    @PreUpdate
    void setUpdateTime() {
        updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public GateStatus getStatus() {
        return status;
    }

    public Instant getOpensAt() {
        return opensAt;
    }

    public Instant getClosesAt() {
        return closesAt;
    }

    public int getAdmissionRate() {
        return admissionRate;
    }

    public int getMaxActiveUsers() {
        return maxActiveUsers;
    }

    public int getSessionTtlSeconds() {
        return sessionTtlSeconds;
    }

    public int getTokenTtlSeconds() {
        return tokenTtlSeconds;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public long getVersion() {
        return version;
    }
}
