package com.lazorenkoo.crowdgate.gate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.time.Instant;

public interface GateRepository extends JpaRepository<Gate, UUID> {

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Gate g set g.status = com.lazorenkoo.crowdgate.gate.GateStatus.CLOSED,
                g.updatedAt = :now, g.version = g.version + 1
            where g.status <> com.lazorenkoo.crowdgate.gate.GateStatus.CLOSED
                and g.closesAt <= :now
            """)
    int closeDue(@Param("now") Instant now);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Gate g set g.status = com.lazorenkoo.crowdgate.gate.GateStatus.OPEN,
                g.updatedAt = :now, g.version = g.version + 1
            where g.status = com.lazorenkoo.crowdgate.gate.GateStatus.SCHEDULED
                and g.opensAt <= :now and (g.closesAt is null or g.closesAt > :now)
            """)
    int openDue(@Param("now") Instant now);
}
