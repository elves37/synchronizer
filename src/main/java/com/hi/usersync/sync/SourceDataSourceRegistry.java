package com.hi.usersync.sync;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SourceDataSourceRegistry {
    private final SyncSourceDbRepository sourceDbRepository;
    private final PasswordResolver passwordResolver;
    private final Map<String, HikariDataSource> cache = new ConcurrentHashMap<>();

    public SourceDataSourceRegistry(SyncSourceDbRepository sourceDbRepository, PasswordResolver passwordResolver) {
        this.sourceDbRepository = sourceDbRepository;
        this.passwordResolver = passwordResolver;
    }

    public DataSource getDataSource(String dbKey) {
        return cache.computeIfAbsent(dbKey, this::createDataSource);
    }

    private HikariDataSource createDataSource(String dbKey) {
        SyncSourceDb sourceDb = sourceDbRepository.findByDbKey(dbKey)
                .orElseThrow(() -> new IllegalArgumentException("Unknown source db key: " + dbKey));
        if (!sourceDb.enabled()) {
            throw new IllegalStateException("Disabled source db: " + dbKey);
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(sourceDb.jdbcUrl());
        config.setUsername(sourceDb.username());
        config.setPassword(passwordResolver.resolve(sourceDb.passwordSecretKey()));
        config.setDriverClassName(sourceDb.driverClassName());
        config.setPoolName("source-" + dbKey);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setReadOnly(true);
        return new HikariDataSource(config);
    }

    @PreDestroy
    public void close() {
        cache.values().forEach(HikariDataSource::close);
    }
}
