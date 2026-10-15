package com.lazorenkoo.crowdgate.gate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.time.Instant;
import java.util.List;

public interface GateRepository extends JpaRepository<Gate, UUID> {

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Gate g set g.status = com.lazorenkoo.crowdgate.gate.GateStatus.CLOSED,
                g.updatedAt = :now, g.version = g.version + 1, g.queueClosePending = true
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

    @Query("select g from Gate g where g.queueClosePending = true order by g.id")
    List<Gate> findPendingQueueCloses(Pageable pageable);

    @Query("""
            select g from Gate g
            where g.status = com.lazorenkoo.crowdgate.gate.GateStatus.OPEN
                and (g.closesAt is null or g.closesAt > :now)
                and (:after is null or g.id > :after)
            order by g.id
            """)
    List<Gate> findAdmissionGates(@Param("now") Instant now, @Param("after") UUID after, Pageable pageable);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Gate g set g.queueClosePending = false
            where g.id = :id and g.status = com.lazorenkoo.crowdgate.gate.GateStatus.CLOSED
                and g.queueClosePending = true
            """)
    int acknowledgeQueueClose(@Param("id") UUID id);
}
