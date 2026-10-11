package com.hi.usersync.sync;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Repository
public class ExcludedAccountRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public ExcludedAccountRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Set<String> findExcludedKeys(Long sourceId) {
        String sql = """
                SELECT account_key
                FROM sync_excluded_account
                WHERE source_id IS NULL OR source_id = :sourceId
                """;
        return new HashSet<>(jdbc.queryForList(sql, Map.of("sourceId", sourceId), String.class));
    }

    public void excludeGlobal(String accountKey, String reason, String userId) {
        String sql = """
                INSERT INTO sync_excluded_account (
                    account_key, source_id, reason, manual_editable, created_by, updated_by
                )
                VALUES (
                    :accountKey, NULL, :reason, TRUE, :userId, :userId
                )
                ON CONFLICT (account_key) WHERE source_id IS NULL
                DO UPDATE SET
                    reason = EXCLUDED.reason,
                    manual_editable = TRUE,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = CURRENT_TIMESTAMP
                """;
        jdbc.update(sql, Map.of(
                "accountKey", accountKey,
                "reason", reason == null ? "" : reason,
                "userId", userId == null ? "system" : userId
        ));
    }

    public void reinclude(String accountKey, Long sourceId) {
        String sql;
        Map<String, Object> params;
        if (sourceId == null) {
            sql = "DELETE FROM sync_excluded_account WHERE account_key = :accountKey";
            params = Map.of("accountKey", accountKey);
        } else {
            sql = """
                    DELETE FROM sync_excluded_account
                    WHERE account_key = :accountKey
                      AND (source_id IS NULL OR source_id = :sourceId)
                    """;
            params = Map.of("accountKey", accountKey, "sourceId", sourceId);
        }
        jdbc.update(sql, params);
    }
}
