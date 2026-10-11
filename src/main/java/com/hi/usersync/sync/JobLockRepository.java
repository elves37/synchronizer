package com.hi.usersync.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Repository
public class JobLockRepository {
    private final NamedParameterJdbcTemplate jdbc;
    private final String owner;
    private final int ttlSeconds;

    public JobLockRepository(NamedParameterJdbcTemplate jdbc,
                             @Value("${sync.lock.ttl-seconds:1800}") int ttlSeconds) {
        this.jdbc = jdbc;
        this.ttlSeconds = ttlSeconds;
        this.owner = resolveOwner();
    }

    public boolean tryLock(String lockKey) {
        String insert = """
                INSERT INTO sync_job_lock (lock_key, locked_until, locked_by)
                VALUES (:lockKey, TIMESTAMP '1970-01-01 00:00:00', '')
                ON CONFLICT (lock_key) DO NOTHING
                """;
        jdbc.update(insert, Map.of("lockKey", lockKey));

        String update = """
                UPDATE sync_job_lock
                SET locked_until = :lockedUntil,
                    locked_by = :owner,
                    updated_at = CURRENT_TIMESTAMP
                WHERE lock_key = :lockKey
                  AND locked_until < CURRENT_TIMESTAMP
                """;
        int updated = jdbc.update(update, Map.of(
                "lockKey", lockKey,
                "owner", owner,
                "lockedUntil", LocalDateTime.now().plusSeconds(ttlSeconds)
        ));
        return updated == 1;
    }

    public void release(String lockKey) {
        String sql = """
                UPDATE sync_job_lock
                SET locked_until = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE lock_key = :lockKey
                  AND locked_by = :owner
                """;
        jdbc.update(sql, Map.of("lockKey", lockKey, "owner", owner));
    }

    private String resolveOwner() {
        try {
            return InetAddress.getLocalHost().getHostName() + ":" + UUID.randomUUID();
        } catch (Exception e) {
            return "unknown:" + UUID.randomUUID();
        }
    }
}
