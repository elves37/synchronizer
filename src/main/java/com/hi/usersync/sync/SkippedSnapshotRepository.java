package com.hi.usersync.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Map;

@Repository
public class SkippedSnapshotRepository {
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public SkippedSnapshotRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void upsert(Long sourceId, String accountKey, Map<String, Object> row, String rowHash,
                       LocalDateTime syncUpdatedAt, Long runId) {
        try {
            String payload = objectMapper.writeValueAsString(row);
            String sql = """
                    INSERT INTO sync_skipped_snapshot (
                        source_id, account_key, source_payload, row_hash, sync_updated_at,
                        last_seen_run_id, last_seen_at
                    )
                    VALUES (
                        :sourceId, :accountKey, CAST(:payload AS jsonb), :rowHash, :syncUpdatedAt,
                        :runId, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (source_id, account_key)
                    DO UPDATE SET
                        source_payload = EXCLUDED.source_payload,
                        row_hash = EXCLUDED.row_hash,
                        sync_updated_at = EXCLUDED.sync_updated_at,
                        last_seen_run_id = EXCLUDED.last_seen_run_id,
                        last_seen_at = CURRENT_TIMESTAMP
                    """;
            jdbc.update(sql, Map.of(
                    "sourceId", sourceId,
                    "accountKey", accountKey,
                    "payload", payload,
                    "rowHash", rowHash,
                    "syncUpdatedAt", syncUpdatedAt,
                    "runId", runId
            ));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save skipped snapshot", e);
        }
    }
}
