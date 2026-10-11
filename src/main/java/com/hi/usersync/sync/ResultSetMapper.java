package com.hi.usersync.sync;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class ResultSetMapper {
    private ResultSetMapper() {
    }

    public static Map<String, Object> toNormalizedMap(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>();

        for (int i = 1; i <= columnCount; i++) {
            String label = metaData.getColumnLabel(i);
            if (label == null || label.isBlank()) {
                label = metaData.getColumnName(i);
            }
            row.put(normalizeKey(label), rs.getObject(i));
        }
        return row;
    }

    public static String normalizeKey(String key) {
        if (key == null) {
            return null;
        }
        return key.trim().toLowerCase(Locale.ROOT);
    }
}
