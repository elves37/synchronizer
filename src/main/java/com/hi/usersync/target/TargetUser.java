package com.hi.usersync.target;

import java.time.LocalDateTime;

public record TargetUser(
        String accountKey,
        String loginId,
        String name,
        String email,
        String phone,
        String addressText,
        String organizationName,
        String extraPayload,
        String status,
        LocalDateTime sourceUpdatedAt,
        String syncMode,
        LocalDateTime lastSyncedAt,
        LocalDateTime manualUpdatedAt,
        String manualUpdatedBy
) {
}
