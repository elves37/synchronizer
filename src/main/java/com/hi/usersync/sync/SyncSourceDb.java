package com.hi.usersync.sync;

public record SyncSourceDb(
        String dbKey,
        String name,
        String jdbcUrl,
        String username,
        String passwordSecretKey,
        String driverClassName,
        boolean enabled
) {
}
