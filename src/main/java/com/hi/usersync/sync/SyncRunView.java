package com.hi.usersync.sync;

import java.time.LocalDateTime;

public record SyncRunView(
        Long id,
        Long sourceId,
        String triggerType,
        String status,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        int totalCount,
        int upsertedCount,
        int unchangedCount,
        int skippedExcludedCount,
        int failedCount,
        String errorMessage
) {
}
