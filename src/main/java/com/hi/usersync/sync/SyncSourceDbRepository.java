package com.hi.usersync.sync;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;

@Repository
public class SyncSourceDbRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public SyncSourceDbRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<SyncSourceDb> findByDbKey(String dbKey) {
        String sql = """
                SELECT db_key, name, jdbc_url, username, password_secret_key, driver_class_name, enabled
                FROM sync_source_db
                WHERE db_key = :dbKey
                """;
        return jdbc.query(sql, Map.of("dbKey", dbKey), rs -> {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new SyncSourceDb(
                    rs.getString("db_key"),
                    rs.getString("name"),
                    rs.getString("jdbc_url"),
                    rs.getString("username"),
                    rs.getString("password_secret_key"),
                    rs.getString("driver_class_name"),
                    rs.getBoolean("enabled")
            ));
        });
    }
}
