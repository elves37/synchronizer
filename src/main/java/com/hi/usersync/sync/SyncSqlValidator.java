package com.hi.usersync.sync;

import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
public class SyncSqlValidator {
    public String validateFetchSql(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("fetch sql is empty");
        }

        String normalized = sql.trim().toLowerCase(Locale.ROOT);
        if (!(normalized.startsWith("select") || normalized.startsWith("with"))) {
            throw new IllegalArgumentException("fetch sql must start with SELECT or WITH");
        }

        if (normalized.contains(";")) {
            throw new IllegalArgumentException("fetch sql must not contain semicolon");
        }

        String padded = " " + normalized.replaceAll("\\s+", " ") + " ";
        String[] blocked = {
                " insert ", " update ", " delete ", " drop ", " alter ", " truncate ",
                " create ", " grant ", " revoke ", " merge ", " call "
        };
        for (String keyword : blocked) {
            if (padded.contains(keyword)) {
                throw new IllegalArgumentException("fetch sql contains blocked keyword: " + keyword.trim());
            }
        }

        if (!normalized.contains(":last_updated_at") || !normalized.contains(":current_cutoff")) {
            throw new IllegalArgumentException("incremental sql must contain :last_updated_at and :current_cutoff parameters");
        }
        if (!normalized.contains("order by")) {
            throw new IllegalArgumentException("incremental sql must contain ORDER BY updated_at, account_key");
        }
        if (!normalized.contains(":chunk_size")) {
            throw new IllegalArgumentException("incremental sql must contain LIMIT :chunk_size");
        }
        return sql;
    }

    public String validateFetchOneSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return sql;
        }
        String normalized = sql.trim().toLowerCase(Locale.ROOT);
        if (!(normalized.startsWith("select") || normalized.startsWith("with"))) {
            throw new IllegalArgumentException("fetch_one_sql must start with SELECT or WITH");
        }
        if (normalized.contains(";") || !normalized.contains(":account_key")) {
            throw new IllegalArgumentException("fetch_one_sql must contain :account_key and must not contain semicolon");
        }
        return sql;
    }
}
