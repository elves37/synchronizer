package com.hi.usersync.sync;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class SyncSourceConfigRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public SyncSourceConfigRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<SyncSourceConfig> findAll() {
        String sql = """
                SELECT *
                FROM sync_source_config
                ORDER BY id
                """;
        return jdbc.query(sql, rowMapper());
    }

    public Optional<SyncSourceConfig> findById(Long id) {
        String sql = "SELECT * FROM sync_source_config WHERE id = :id";
        return jdbc.query(sql, Map.of("id", id), rs -> rs.next() ? Optional.of(mapRow(rs)) : Optional.empty());
    }

    /**
     * Returns currently due source IDs. A returned ID is not owned yet; claimScheduledRun must succeed
     * before it is submitted to the worker pool.
     */
    public List<Long> findDueSourceIds(int limit) {
        String sql = """
                SELECT id
                FROM sync_source_config
                WHERE enabled = TRUE
                  AND schedule_enabled = TRUE
                  AND next_run_at IS NOT NULL
                  AND next_run_at <= CURRENT_TIMESTAMP
                ORDER BY next_run_at ASC, id ASC
                LIMIT :limit
                """;
        return jdbc.queryForList(sql, Map.of("limit", Math.max(limit, 1)), Long.class);
    }

    /**
     * Atomically claims one due schedule and advances its next execution time. This conditional update
     * prevents multiple application instances from dispatching the same due schedule.
     */
    public boolean claimScheduledRun(Long id) {
        String sql = """
                UPDATE sync_source_config
                SET last_scheduled_at = CURRENT_TIMESTAMP,
                    next_run_at = CURRENT_TIMESTAMP
                        + (schedule_interval_minutes * INTERVAL '1 minute'),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                  AND enabled = TRUE
                  AND schedule_enabled = TRUE
                  AND next_run_at IS NOT NULL
                  AND next_run_at <= CURRENT_TIMESTAMP
                """;
        return jdbc.update(sql, Map.of("id", id)) == 1;
    }

    /**
     * If the local executor rejects a claimed task, retry the dispatch shortly instead of waiting for
     * the full configured interval.
     */
    public void postponeAfterDispatchFailure(Long id) {
        String sql = """
                UPDATE sync_source_config
                SET next_run_at = CASE
                        WHEN enabled = TRUE AND schedule_enabled = TRUE
                            THEN CURRENT_TIMESTAMP + INTERVAL '1 minute'
                        ELSE NULL
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """;
        jdbc.update(sql, Map.of("id", id));
    }

    public Long create(SyncSourceConfig config) {
        String sql = """
                INSERT INTO sync_source_config (
                    source_db_key, name, incremental_sql, fetch_one_sql,
                    key_alias, updated_at_alias, upsert_sql, chunk_size, fetch_size,
                    safety_lag_seconds, overlap_seconds, enabled, schedule_enabled,
                    schedule_interval_minutes, next_run_at
                )
                VALUES (
                    :sourceDbKey, :name, :incrementalSql, :fetchOneSql,
                    :keyAlias, :updatedAtAlias, :upsertSql, :chunkSize, :fetchSize,
                    :safetyLagSeconds, :overlapSeconds, :enabled, :scheduleEnabled,
                    :scheduleIntervalMinutes,
                    CASE
                        WHEN :enabled = TRUE AND :scheduleEnabled = TRUE
                            THEN CURRENT_TIMESTAMP
                                + (:scheduleIntervalMinutes * INTERVAL '1 minute')
                        ELSE NULL
                    END
                )
                """;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(sql, params(config), keyHolder, new String[]{"id"});
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("Failed to create sync source config");
        }
        return key.longValue();
    }

    public void update(Long id, SyncSourceConfig config) {
        String sql = """
                UPDATE sync_source_config
                SET source_db_key = :sourceDbKey,
                    name = :name,
                    incremental_sql = :incrementalSql,
                    fetch_one_sql = :fetchOneSql,
                    key_alias = :keyAlias,
                    updated_at_alias = :updatedAtAlias,
                    upsert_sql = :upsertSql,
                    chunk_size = :chunkSize,
                    fetch_size = :fetchSize,
                    safety_lag_seconds = :safetyLagSeconds,
                    overlap_seconds = :overlapSeconds,
                    enabled = :enabled,
                    schedule_enabled = :scheduleEnabled,
                    schedule_interval_minutes = :scheduleIntervalMinutes,
                    next_run_at = CASE
                        WHEN :enabled = FALSE OR :scheduleEnabled = FALSE THEN NULL
                        WHEN enabled IS DISTINCT FROM :enabled
                          OR schedule_enabled IS DISTINCT FROM :scheduleEnabled
                          OR schedule_interval_minutes IS DISTINCT FROM :scheduleIntervalMinutes
                          OR next_run_at IS NULL
                            THEN CURRENT_TIMESTAMP
                                + (:scheduleIntervalMinutes * INTERVAL '1 minute')
                        ELSE next_run_at
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """;
        MapSqlParameterSource params = params(config).addValue("id", id);
        int updated = jdbc.update(sql, params);
        if (updated == 0) {
            throw new IllegalArgumentException("Unknown source id: " + id);
        }
    }

    public void updateSchedule(Long id, boolean scheduleEnabled, int intervalMinutes) {
        String sql = """
                UPDATE sync_source_config
                SET schedule_enabled = :scheduleEnabled,
                    schedule_interval_minutes = :intervalMinutes,
                    next_run_at = CASE
                        WHEN enabled = TRUE AND :scheduleEnabled = TRUE
                            THEN CURRENT_TIMESTAMP + (:intervalMinutes * INTERVAL '1 minute')
                        ELSE NULL
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """;
        int updated = jdbc.update(sql, Map.of(
                "id", id,
                "scheduleEnabled", scheduleEnabled,
                "intervalMinutes", intervalMinutes
        ));
        if (updated == 0) {
            throw new IllegalArgumentException("Unknown source id: " + id);
        }
    }

    private MapSqlParameterSource params(SyncSourceConfig config) {
        return new MapSqlParameterSource()
                .addValue("sourceDbKey", config.sourceDbKey())
                .addValue("name", config.name())
                .addValue("incrementalSql", config.incrementalSql())
                .addValue("fetchOneSql", config.fetchOneSql())
                .addValue("keyAlias", config.keyAlias())
                .addValue("updatedAtAlias", config.updatedAtAlias())
                .addValue("upsertSql", config.upsertSql())
                .addValue("chunkSize", config.chunkSize())
                .addValue("fetchSize", config.fetchSize())
                .addValue("safetyLagSeconds", config.safetyLagSeconds())
                .addValue("overlapSeconds", config.overlapSeconds())
                .addValue("enabled", config.enabled())
                .addValue("scheduleEnabled", config.scheduleEnabled())
                .addValue("scheduleIntervalMinutes", config.scheduleIntervalMinutes());
    }

    private RowMapper<SyncSourceConfig> rowMapper() {
        return (rs, rowNum) -> mapRow(rs);
    }

    private SyncSourceConfig mapRow(ResultSet rs) throws SQLException {
        return new SyncSourceConfig(
                rs.getLong("id"),
                rs.getString("source_db_key"),
                rs.getString("name"),
                rs.getString("incremental_sql"),
                rs.getString("fetch_one_sql"),
                rs.getString("key_alias"),
                rs.getString("updated_at_alias"),
                rs.getString("upsert_sql"),
                rs.getInt("chunk_size"),
                rs.getInt("fetch_size"),
                rs.getInt("safety_lag_seconds"),
                rs.getInt("overlap_seconds"),
                rs.getBoolean("enabled"),
                rs.getBoolean("schedule_enabled"),
                rs.getInt("schedule_interval_minutes"),
                toLocalDateTime(rs.getTimestamp("next_run_at")),
                toLocalDateTime(rs.getTimestamp("last_scheduled_at")),
                toLocalDateTime(rs.getTimestamp("created_at")),
                toLocalDateTime(rs.getTimestamp("updated_at"))
        );
    }

    private java.time.LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
