package com.hi.usersync.target;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class TargetUserRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public TargetUserRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<TargetUser> search(String keyword, String syncMode, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM target_user WHERE 1=1");
        Map<String, Object> params = new HashMap<>();
        if (keyword != null && !keyword.isBlank()) {
            sql.append("""
                    AND (
                        account_key ILIKE :keyword
                        OR login_id ILIKE :keyword
                        OR name ILIKE :keyword
                        OR email ILIKE :keyword
                        OR address_text ILIKE :keyword
                        OR organization_name ILIKE :keyword
                    )
                    """);
            params.put("keyword", "%" + keyword + "%");
        }
        if (syncMode != null && !syncMode.isBlank()) {
            sql.append(" AND sync_mode = :syncMode");
            params.put("syncMode", syncMode);
        }
        sql.append(" ORDER BY account_key LIMIT :limit");
        params.put("limit", Math.min(Math.max(limit, 1), 500));
        return jdbc.query(sql.toString(), params, rowMapper());
    }

    public Optional<TargetUser> findByAccountKey(String accountKey) {
        String sql = "SELECT * FROM target_user WHERE account_key = :accountKey";
        return jdbc.query(sql, Map.of("accountKey", accountKey), rs -> rs.next() ? Optional.of(mapRow(rs)) : Optional.empty());
    }

    private RowMapper<TargetUser> rowMapper() {
        return (rs, rowNum) -> mapRow(rs);
    }

    private TargetUser mapRow(ResultSet rs) throws SQLException {
        return new TargetUser(
                rs.getString("account_key"),
                rs.getString("login_id"),
                rs.getString("name"),
                rs.getString("email"),
                rs.getString("phone"),
                rs.getString("address_text"),
                rs.getString("organization_name"),
                rs.getString("extra_payload"),
                rs.getString("status"),
                rs.getTimestamp("source_updated_at") == null ? null : rs.getTimestamp("source_updated_at").toLocalDateTime(),
                rs.getString("sync_mode"),
                rs.getTimestamp("last_synced_at") == null ? null : rs.getTimestamp("last_synced_at").toLocalDateTime(),
                rs.getTimestamp("manual_updated_at") == null ? null : rs.getTimestamp("manual_updated_at").toLocalDateTime(),
                rs.getString("manual_updated_by")
        );
    }
}
