package com.hi.usersync.sync;

import java.time.LocalDateTime;

public record SyncSourceConfig(
        Long id,
        String sourceDbKey,
        String name,
        String incrementalSql,
        String fetchOneSql,
        String keyAlias,
        String updatedAtAlias,
        String upsertSql,
        int chunkSize,
        int fetchSize,
        int safetyLagSeconds,
        int overlapSeconds,
        boolean enabled,
        boolean scheduleEnabled,
        int scheduleIntervalMinutes,
        LocalDateTime nextRunAt,
        LocalDateTime lastScheduledAt,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
