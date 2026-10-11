package com.hi.usersync.sync;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class TemporalMapper {
    private TemporalMapper() {
    }

    public static LocalDateTime toLocalDateTime(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("timestamp value is null");
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime();
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toLocalDateTime();
        }
        if (value instanceof Instant instant) {
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }
        if (value instanceof java.util.Date date) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        }
        if (value instanceof CharSequence text) {
            String raw = text.toString().trim();
            try {
                return LocalDateTime.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception ignored) {
                return Timestamp.valueOf(raw).toLocalDateTime();
            }
        }
        throw new IllegalArgumentException("Unsupported timestamp type: " + value.getClass().getName());
    }
}
