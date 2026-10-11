package com.hi.usersync.sync;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class SyncRunRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public SyncRunRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long start(Long sourceId, String triggerType) {
        String sql = """
                INSERT INTO sync_run (source_id, trigger_type, status)
                VALUES (:sourceId, :triggerType, 'RUNNING')
                """;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(sql, new MapSqlParameterSource()
                .addValue("sourceId", sourceId)
                .addValue("triggerType", triggerType), keyHolder, new String[]{"id"});
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("Failed to create sync run");
        }
        return key.longValue();
    }

    public void success(Long runId, SyncCounter counter) {
        String sql = """
                UPDATE sync_run
                SET status = 'SUCCESS',
                    finished_at = CURRENT_TIMESTAMP,
                    total_count = :totalCount,
                    upserted_count = :upsertedCount,
                    unchanged_count = :unchangedCount,
                    skipped_excluded_count = :skippedExcludedCount,
                    failed_count = :failedCount,
                    error_message = NULL
                WHERE id = :runId
                """;
        jdbc.update(sql, Map.of(
                "runId", runId,
                "totalCount", counter.totalCount(),
                "upsertedCount", counter.upsertedCount(),
                "unchangedCount", counter.unchangedCount(),
                "skippedExcludedCount", counter.skippedExcludedCount(),
                "failedCount", counter.failedCount()
        ));
    }

    public void fail(Long runId, String message) {
        String sql = """
                UPDATE sync_run
                SET status = 'FAILED',
                    finished_at = CURRENT_TIMESTAMP,
                    error_message = :message
                WHERE id = :runId
                """;
        jdbc.update(sql, Map.of("runId", runId, "message", message == null ? "" : message));
    }

    public List<SyncRunView> findRuns(Long sourceId, int limit) {
        String base = """
                SELECT *
                FROM sync_run
                %s
                ORDER BY id DESC
                LIMIT :limit
                """;
        Map<String, Object> params = new HashMap<>();
        params.put("limit", Math.min(Math.max(limit, 1), 500));
        String where = "";
        if (sourceId != null) {
            where = "WHERE source_id = :sourceId";
            params.put("sourceId", sourceId);
        }
        return jdbc.query(base.formatted(where), params, rowMapper());
    }

    public Optional<SyncRunView> findById(Long id) {
        String sql = "SELECT * FROM sync_run WHERE id = :id";
        return jdbc.query(sql, Map.of("id", id), rs -> rs.next() ? Optional.of(mapRow(rs)) : Optional.empty());
    }

    private RowMapper<SyncRunView> rowMapper() {
        return (rs, rowNum) -> mapRow(rs);
    }

    private SyncRunView mapRow(ResultSet rs) throws SQLException {
        return new SyncRunView(
                rs.getLong("id"),
                rs.getLong("source_id"),
                rs.getString("trigger_type"),
                rs.getString("status"),
                rs.getTimestamp("started_at").toLocalDateTime(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toLocalDateTime(),
                rs.getInt("total_count"),
                rs.getInt("upserted_count"),
                rs.getInt("unchanged_count"),
                rs.getInt("skipped_excluded_count"),
                rs.getInt("failed_count"),
                rs.getString("error_message")
        );
    }
}
