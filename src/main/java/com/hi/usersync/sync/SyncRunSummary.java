package com.hi.usersync.sync;

public record SyncRunSummary(
        Long runId,
        Long sourceId,
        String status,
        int totalCount,
        int upsertedCount,
        int unchangedCount,
        int skippedExcludedCount,
        int failedCount
) {
}
