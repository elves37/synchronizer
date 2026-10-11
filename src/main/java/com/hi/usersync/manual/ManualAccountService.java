package com.hi.usersync.manual;

import com.hi.usersync.sync.ExcludedAccountRepository;
import com.hi.usersync.sync.SyncExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ManualAccountService {
    private static final Set<String> ALLOWED_COLUMNS = Set.of(
            "login_id", "name", "email", "phone", "address_text", "organization_name", "status"
    );

    private final NamedParameterJdbcTemplate jdbc;
    private final ExcludedAccountRepository excludedAccountRepository;
    private final SyncExecutor syncExecutor;
    private final ObjectMapper objectMapper;

    public ManualAccountService(NamedParameterJdbcTemplate jdbc,
                                ExcludedAccountRepository excludedAccountRepository,
                                SyncExecutor syncExecutor,
                                ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.excludedAccountRepository = excludedAccountRepository;
        this.syncExecutor = syncExecutor;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void exclude(String accountKey, String reason, String userId) {
        excludedAccountRepository.excludeGlobal(accountKey, reason, userId);
        ensureTargetExistsAsManual(accountKey, userId);
    }

    @Transactional
    public void excludeAndUpdate(String accountKey, Map<String, Object> patch, String reason, String userId) {
        validatePatch(patch);
        Map<String, Object> before = findTargetPayload(accountKey);

        excludedAccountRepository.excludeGlobal(accountKey, reason, userId);
        ensureTargetExistsAsManual(accountKey, userId);

        if (patch != null && !patch.isEmpty()) {
            String setClause = patch.keySet().stream()
                    .map(column -> column + " = :" + column)
                    .collect(Collectors.joining(", "));
            String sql = """
                    UPDATE target_user
                    SET %s,
                        sync_mode = 'MANUAL',
                        manual_updated_by = :manualUpdatedBy,
                        manual_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE account_key = :accountKey
                    """.formatted(setClause);
            Map<String, Object> params = new LinkedHashMap<>(patch);
            params.put("accountKey", accountKey);
            params.put("manualUpdatedBy", userId == null ? "system" : userId);
            jdbc.update(sql, params);
        }

        Map<String, Object> after = findTargetPayload(accountKey);
        saveChangeLog(accountKey, userId, before, after, reason);
    }

    @Transactional
    public void reinclude(String accountKey, Long sourceId, String userId) {
        excludedAccountRepository.reinclude(accountKey, sourceId);
        String sql = """
                UPDATE target_user
                SET sync_mode = 'SYNC',
                    updated_at = CURRENT_TIMESTAMP
                WHERE account_key = :accountKey
                """;
        jdbc.update(sql, Map.of("accountKey", accountKey));

        if (sourceId != null) {
            syncExecutor.syncOne(sourceId, accountKey);
        }
    }

    private void ensureTargetExistsAsManual(String accountKey, String userId) {
        String sql = """
                INSERT INTO target_user (
                    account_key, sync_mode, manual_updated_by, manual_updated_at, updated_at
                )
                VALUES (
                    :accountKey, 'MANUAL', :userId, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (account_key)
                DO UPDATE SET
                    sync_mode = 'MANUAL',
                    manual_updated_by = EXCLUDED.manual_updated_by,
                    manual_updated_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """;
        jdbc.update(sql, Map.of(
                "accountKey", accountKey,
                "userId", userId == null ? "system" : userId
        ));
    }

    private void validatePatch(Map<String, Object> patch) {
        if (patch == null) {
            return;
        }
        for (String column : patch.keySet()) {
            if (!ALLOWED_COLUMNS.contains(column)) {
                throw new IllegalArgumentException("Manual update is not allowed for column: " + column);
            }
        }
    }

    private Map<String, Object> findTargetPayload(String accountKey) {
        String sql = """
                SELECT account_key, login_id, name, email, phone, address_text, organization_name, extra_payload,
                       status, source_updated_at, sync_mode, last_synced_at, manual_updated_at, manual_updated_by
                FROM target_user
                WHERE account_key = :accountKey
                """;
        return jdbc.query(sql, Map.of("accountKey", accountKey), rs -> {
            if (!rs.next()) {
                return Map.of();
            }
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("account_key", rs.getString("account_key"));
            map.put("login_id", rs.getString("login_id"));
            map.put("name", rs.getString("name"));
            map.put("email", rs.getString("email"));
            map.put("phone", rs.getString("phone"));
            map.put("address_text", rs.getString("address_text"));
            map.put("organization_name", rs.getString("organization_name"));
            map.put("extra_payload", rs.getString("extra_payload"));
            map.put("status", rs.getString("status"));
            map.put("source_updated_at", rs.getTimestamp("source_updated_at"));
            map.put("sync_mode", rs.getString("sync_mode"));
            map.put("last_synced_at", rs.getTimestamp("last_synced_at"));
            map.put("manual_updated_at", rs.getTimestamp("manual_updated_at"));
            map.put("manual_updated_by", rs.getString("manual_updated_by"));
            return map;
        });
    }

    private void saveChangeLog(String accountKey, String userId, Map<String, Object> before,
                               Map<String, Object> after, String reason) {
        try {
            String sql = """
                    INSERT INTO sync_manual_change_log (
                        account_key, changed_by, before_payload, after_payload, reason
                    )
                    VALUES (
                        :accountKey, :changedBy, CAST(:beforePayload AS jsonb), CAST(:afterPayload AS jsonb), :reason
                    )
                    """;
            jdbc.update(sql, Map.of(
                    "accountKey", accountKey,
                    "changedBy", userId == null ? "system" : userId,
                    "beforePayload", objectMapper.writeValueAsString(before == null ? Map.of() : before),
                    "afterPayload", objectMapper.writeValueAsString(after == null ? Map.of() : after),
                    "reason", reason == null ? "" : reason
            ));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save manual change log", e);
        }
    }
}
