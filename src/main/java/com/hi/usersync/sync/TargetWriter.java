package com.hi.usersync.sync;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class TargetWriter {
    private final NamedParameterJdbcTemplate jdbc;

    public TargetWriter(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int upsert(SyncSourceConfig config, List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        MapSqlParameterSource[] params = rows.stream()
                .map(MapSqlParameterSource::new)
                .toArray(MapSqlParameterSource[]::new);
        int[] results = jdbc.batchUpdate(config.upsertSql(), params);
        int affected = 0;
        for (int result : results) {
            if (result > 0) {
                affected += result;
            }
        }
        return affected;
    }
}
