package com.hi.usersync.sync;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;

@Repository
public class SyncAccountStateRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public SyncAccountStateRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean isSameHash(Long sourceId, String accountKey, String rowHash) {
        String sql = """
                SELECT row_hash
                FROM sync_account_state
                WHERE source_id = :sourceId
                  AND account_key = :accountKey
                """;
        Optional<String> oldHash = jdbc.query(sql, Map.of(
                "sourceId", sourceId,
                "accountKey", accountKey
        ), rs -> rs.next() ? Optional.ofNullable(rs.getString("row_hash")) : Optional.empty());
        return oldHash.isPresent() && oldHash.get().equals(rowHash);
    }

    public void markSynced(Long sourceId, String accountKey, String rowHash, Long runId) {
        String sql = """
                INSERT INTO sync_account_state (
                    source_id, account_key, row_hash, status, last_seen_run_id,
                    last_seen_at, last_synced_at, error_message
                )
                VALUES (
                    :sourceId, :accountKey, :rowHash, 'SYNCED', :runId,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL
                )
                ON CONFLICT (source_id, account_key)
                DO UPDATE SET
                    row_hash = EXCLUDED.row_hash,
                    status = 'SYNCED',
                    last_seen_run_id = EXCLUDED.last_seen_run_id,
                    last_seen_at = CURRENT_TIMESTAMP,
                    last_synced_at = CURRENT_TIMESTAMP,
                    error_message = NULL
                """;
        jdbc.update(sql, Map.of(
                "sourceId", sourceId,
                "accountKey", accountKey,
                "rowHash", rowHash,
                "runId", runId
        ));
    }

    public void markStatus(Long sourceId, String accountKey, Long runId, String status) {
        String sql = """
                INSERT INTO sync_account_state (
                    source_id, account_key, status, last_seen_run_id, last_seen_at
                )
                VALUES (
                    :sourceId, :accountKey, :status, :runId, CURRENT_TIMESTAMP
                )
                ON CONFLICT (source_id, account_key)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    last_seen_run_id = EXCLUDED.last_seen_run_id,
                    last_seen_at = CURRENT_TIMESTAMP
                """;
        jdbc.update(sql, Map.of(
                "sourceId", sourceId,
                "accountKey", accountKey,
                "status", status,
                "runId", runId
        ));
    }
}
