package com.hi.usersync.sync;

import java.time.LocalDateTime;

public record SyncCursor(
        Long sourceId,
        LocalDateTime lastUpdatedAt,
        String lastAccountKey,
        LocalDateTime lastCutoffAt,
        Long lastRunId
) {
}
