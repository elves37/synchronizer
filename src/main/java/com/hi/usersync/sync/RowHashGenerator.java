package com.hi.usersync.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;

@Component
public class RowHashGenerator {
    private final ObjectMapper objectMapper;

    public RowHashGenerator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String generate(Map<String, Object> row) {
        try {
            Map<String, Object> sorted = new TreeMap<>(row);
            sorted.remove("_source_id");
            sorted.remove("_sync_run_id");
            sorted.remove("_row_hash");
            String json = objectMapper.writeValueAsString(sorted);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashed) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate row hash", e);
        }
    }
}
