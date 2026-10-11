package com.hi.usersync.sync;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Map;

@Repository
public class SyncCursorRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public SyncCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public SyncCursor getOrCreate(Long sourceId) {
        String insert = """
                INSERT INTO sync_cursor (source_id, last_updated_at, last_account_key)
                VALUES (:sourceId, TIMESTAMP '1970-01-01 00:00:00', '')
                ON CONFLICT (source_id) DO NOTHING
                """;
        jdbc.update(insert, Map.of("sourceId", sourceId));

        String select = """
                SELECT source_id, last_updated_at, last_account_key, last_cutoff_at, last_run_id
                FROM sync_cursor
                WHERE source_id = :sourceId
                """;
        return jdbc.query(select, Map.of("sourceId", sourceId), rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("Failed to create cursor for source: " + sourceId);
            }
            return new SyncCursor(
                    rs.getLong("source_id"),
                    rs.getTimestamp("last_updated_at").toLocalDateTime(),
                    rs.getString("last_account_key"),
                    rs.getTimestamp("last_cutoff_at") == null ? null : rs.getTimestamp("last_cutoff_at").toLocalDateTime(),
                    rs.getObject("last_run_id") == null ? null : rs.getLong("last_run_id")
            );
        });
    }

    public void update(Long sourceId, LocalDateTime lastUpdatedAt, String lastAccountKey, LocalDateTime cutoffAt, Long runId) {
        String sql = """
                UPDATE sync_cursor
                SET last_updated_at = :lastUpdatedAt,
                    last_account_key = :lastAccountKey,
                    last_cutoff_at = :cutoffAt,
                    last_run_id = :runId,
                    updated_at = CURRENT_TIMESTAMP
                WHERE source_id = :sourceId
                """;
        jdbc.update(sql, Map.of(
                "sourceId", sourceId,
                "lastUpdatedAt", lastUpdatedAt,
                "lastAccountKey", lastAccountKey,
                "cutoffAt", cutoffAt,
                "runId", runId
        ));
    }
}
