package com.hi.usersync.sync;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class SyncExecutor {
    private final SyncSourceConfigRepository configRepository;
    private final SourceDataSourceRegistry dataSourceRegistry;
    private final SyncCursorRepository cursorRepository;
    private final ExcludedAccountRepository excludedAccountRepository;
    private final SyncAccountStateRepository accountStateRepository;
    private final SkippedSnapshotRepository skippedSnapshotRepository;
    private final SyncRunRepository runRepository;
    private final TargetWriter targetWriter;
    private final RowHashGenerator rowHashGenerator;
    private final SyncSqlValidator sqlValidator;
    private final JobLockRepository jobLockRepository;
    private final TransactionTemplate transactionTemplate;

    public SyncExecutor(SyncSourceConfigRepository configRepository,
                        SourceDataSourceRegistry dataSourceRegistry,
                        SyncCursorRepository cursorRepository,
                        ExcludedAccountRepository excludedAccountRepository,
                        SyncAccountStateRepository accountStateRepository,
                        SkippedSnapshotRepository skippedSnapshotRepository,
                        SyncRunRepository runRepository,
                        TargetWriter targetWriter,
                        RowHashGenerator rowHashGenerator,
                        SyncSqlValidator sqlValidator,
                        JobLockRepository jobLockRepository,
                        TransactionTemplate transactionTemplate) {
        this.configRepository = configRepository;
        this.dataSourceRegistry = dataSourceRegistry;
        this.cursorRepository = cursorRepository;
        this.excludedAccountRepository = excludedAccountRepository;
        this.accountStateRepository = accountStateRepository;
        this.skippedSnapshotRepository = skippedSnapshotRepository;
        this.runRepository = runRepository;
        this.targetWriter = targetWriter;
        this.rowHashGenerator = rowHashGenerator;
        this.sqlValidator = sqlValidator;
        this.jobLockRepository = jobLockRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public SyncRunSummary run(Long sourceId) {
        return runInternal(sourceId, "MANUAL");
    }

    public SyncRunSummary runScheduled(Long sourceId) {
        SyncSourceConfig config = loadConfig(sourceId);
        if (!config.scheduleEnabled()) {
            throw new IllegalStateException("Disabled sync schedule: " + sourceId);
        }
        return runInternal(config, "SCHEDULED");
    }

    public SyncRunSummary syncOne(Long sourceId, String accountKey) {
        SyncSourceConfig config = loadConfig(sourceId);
        if (config.fetchOneSql() == null || config.fetchOneSql().isBlank()) {
            throw new IllegalArgumentException("fetch_one_sql is not configured for source: " + sourceId);
        }
        sqlValidator.validateFetchOneSql(config.fetchOneSql());

        String lockKey = "sync-one:" + sourceId + ":" + accountKey;
        if (!jobLockRepository.tryLock(lockKey)) {
            throw new IllegalStateException("The account sync is already running: " + accountKey);
        }

        Long runId = runRepository.start(sourceId, "REINCLUDE_ONE");
        SyncCounter counter = new SyncCounter();
        try {
            NamedParameterJdbcTemplate sourceJdbc = createSourceJdbc(config);
            List<Map<String, Object>> rows = sourceJdbc.query(
                    config.fetchOneSql(),
                    Map.of("account_key", accountKey),
                    (rs, rowNum) -> ResultSetMapper.toNormalizedMap(rs)
            );
            if (!rows.isEmpty()) {
                LocalDateTime sourceNow = getSourceNow(sourceJdbc);
                processChunk(config, runId, rows, Set.of(), counter, null, sourceNow, false, true);
            }
            runRepository.success(runId, counter);
            return new SyncRunSummary(runId, sourceId, "SUCCESS", counter.totalCount(), counter.upsertedCount(),
                    counter.unchangedCount(), counter.skippedExcludedCount(), counter.failedCount());
        } catch (Exception e) {
            runRepository.fail(runId, e.getMessage());
            throw new IllegalStateException("Single account sync failed. runId=" + runId, e);
        } finally {
            jobLockRepository.release(lockKey);
        }
    }

    private SyncRunSummary runInternal(Long sourceId, String triggerType) {
        return runInternal(loadConfig(sourceId), triggerType);
    }

    private SyncRunSummary runInternal(SyncSourceConfig config, String triggerType) {
        Long sourceId = config.id();
        String lockKey = "sync-source:" + sourceId;
        if (!jobLockRepository.tryLock(lockKey)) {
            throw new IllegalStateException("The sync source is already running: " + sourceId);
        }

        Long runId = runRepository.start(sourceId, triggerType);
        SyncCounter counter = new SyncCounter();
        try {
            String incrementalSql = sqlValidator.validateFetchSql(config.incrementalSql());
            NamedParameterJdbcTemplate sourceJdbc = createSourceJdbc(config);

            SyncCursor cursor = cursorRepository.getOrCreate(sourceId);
            LocalDateTime sourceNow = getSourceNow(sourceJdbc);
            LocalDateTime currentCutoff = sourceNow.minusSeconds(Math.max(config.safetyLagSeconds(), 0));
            LocalDateTime pageUpdatedAt = cursor.lastUpdatedAt().minusSeconds(Math.max(config.overlapSeconds(), 0));
            String pageAccountKey = "";
            CursorTuple persistedCursor = new CursorTuple(cursor.lastUpdatedAt(), cursor.lastAccountKey());

            Set<String> excludedKeys = excludedAccountRepository.findExcludedKeys(sourceId);

            while (true) {
                MapSqlParameterSource params = new MapSqlParameterSource()
                        .addValue("last_updated_at", pageUpdatedAt)
                        .addValue("last_account_key", pageAccountKey)
                        .addValue("current_cutoff", currentCutoff)
                        .addValue("chunk_size", config.chunkSize());

                List<Map<String, Object>> rows = sourceJdbc.query(
                        incrementalSql,
                        params,
                        (rs, rowNum) -> ResultSetMapper.toNormalizedMap(rs)
                );

                if (rows.isEmpty()) {
                    break;
                }

                CursorTuple lastTuple = extractLastTuple(config, rows);
                if (!lastTuple.isAfter(pageUpdatedAt, pageAccountKey)) {
                    throw new IllegalStateException("Cursor did not move. Check ORDER BY and key aliases in source query.");
                }

                if (lastTuple.isAfter(persistedCursor.updatedAt(), persistedCursor.accountKey())) {
                    persistedCursor = lastTuple;
                }

                processChunk(config, runId, rows, excludedKeys, counter, persistedCursor, currentCutoff, true, false);

                pageUpdatedAt = lastTuple.updatedAt();
                pageAccountKey = lastTuple.accountKey();

                if (rows.size() < config.chunkSize()) {
                    break;
                }
            }

            runRepository.success(runId, counter);
            return new SyncRunSummary(runId, sourceId, "SUCCESS", counter.totalCount(), counter.upsertedCount(),
                    counter.unchangedCount(), counter.skippedExcludedCount(), counter.failedCount());
        } catch (Exception e) {
            runRepository.fail(runId, e.getMessage());
            throw new IllegalStateException("Sync failed. sourceId=" + sourceId + ", runId=" + runId, e);
        } finally {
            jobLockRepository.release(lockKey);
        }
    }

    private void processChunk(SyncSourceConfig config,
                              Long runId,
                              List<Map<String, Object>> rows,
                              Set<String> excludedKeys,
                              SyncCounter counter,
                              CursorTuple lastTuple,
                              LocalDateTime currentCutoff,
                              boolean updateCursor,
                              boolean forceWrite) {
        transactionTemplate.executeWithoutResult(status -> {
            List<Map<String, Object>> rowsToWrite = new ArrayList<>();
            String keyAlias = ResultSetMapper.normalizeKey(config.keyAlias());
            String updatedAtAlias = ResultSetMapper.normalizeKey(config.updatedAtAlias());

            for (Map<String, Object> row : rows) {
                Object rawKey = row.get(keyAlias);
                Object rawUpdatedAt = row.get(updatedAtAlias);
                if (rawKey == null || rawKey.toString().isBlank()) {
                    throw new IllegalArgumentException("Source row does not contain key alias: " + config.keyAlias());
                }
                if (rawUpdatedAt == null) {
                    throw new IllegalArgumentException("Source row does not contain updated_at alias: " + config.updatedAtAlias());
                }

                String accountKey = rawKey.toString();
                LocalDateTime syncUpdatedAt = TemporalMapper.toLocalDateTime(rawUpdatedAt);
                String rowHash = rowHashGenerator.generate(row);
                counter.incTotal();

                if (excludedKeys.contains(accountKey)) {
                    skippedSnapshotRepository.upsert(config.id(), accountKey, row, rowHash, syncUpdatedAt, runId);
                    accountStateRepository.markStatus(config.id(), accountKey, runId, "SKIPPED_EXCLUDED");
                    counter.incSkippedExcluded();
                    continue;
                }

                if (!forceWrite && accountStateRepository.isSameHash(config.id(), accountKey, rowHash)) {
                    accountStateRepository.markStatus(config.id(), accountKey, runId, "UNCHANGED");
                    counter.incUnchanged();
                    continue;
                }

                row.put("_source_id", config.id());
                row.put("_sync_run_id", runId);
                row.put("_row_hash", rowHash);
                rowsToWrite.add(row);
            }

            if (!rowsToWrite.isEmpty()) {
                targetWriter.upsert(config, rowsToWrite);
                String normalizedKey = ResultSetMapper.normalizeKey(config.keyAlias());
                for (Map<String, Object> row : rowsToWrite) {
                    String accountKey = row.get(normalizedKey).toString();
                    String rowHash = row.get("_row_hash").toString();
                    accountStateRepository.markSynced(config.id(), accountKey, rowHash, runId);
                    counter.incUpserted();
                }
            }

            if (updateCursor && lastTuple != null) {
                cursorRepository.update(config.id(), lastTuple.updatedAt(), lastTuple.accountKey(), currentCutoff, runId);
            }
        });
    }

    private SyncSourceConfig loadConfig(Long sourceId) {
        SyncSourceConfig config = configRepository.findById(sourceId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown source id: " + sourceId));
        if (!config.enabled()) {
            throw new IllegalStateException("Disabled sync source: " + sourceId);
        }
        return config;
    }

    private NamedParameterJdbcTemplate createSourceJdbc(SyncSourceConfig config) {
        DataSource sourceDataSource = dataSourceRegistry.getDataSource(config.sourceDbKey());
        JdbcTemplate template = new JdbcTemplate(sourceDataSource);
        template.setFetchSize(config.fetchSize());
        template.setMaxRows(Math.max(config.chunkSize(), 1) + 1);
        return new NamedParameterJdbcTemplate(template);
    }

    private LocalDateTime getSourceNow(NamedParameterJdbcTemplate sourceJdbc) {
        Timestamp timestamp = sourceJdbc.getJdbcTemplate().queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class);
        if (timestamp == null) {
            return LocalDateTime.now();
        }
        return timestamp.toLocalDateTime();
    }

    private CursorTuple extractLastTuple(SyncSourceConfig config, List<Map<String, Object>> rows) {
        String keyAlias = ResultSetMapper.normalizeKey(config.keyAlias());
        String updatedAtAlias = ResultSetMapper.normalizeKey(config.updatedAtAlias());
        Map<String, Object> last = rows.get(rows.size() - 1);
        Object rawKey = last.get(keyAlias);
        Object rawUpdatedAt = last.get(updatedAtAlias);
        if (rawKey == null || rawUpdatedAt == null) {
            throw new IllegalArgumentException("Source query must return aliases: " + config.keyAlias() + ", " + config.updatedAtAlias());
        }
        return new CursorTuple(TemporalMapper.toLocalDateTime(rawUpdatedAt), rawKey.toString());
    }

    private record CursorTuple(LocalDateTime updatedAt, String accountKey) {
        boolean isAfter(LocalDateTime oldUpdatedAt, String oldAccountKey) {
            int comparedTime = updatedAt.compareTo(oldUpdatedAt);
            if (comparedTime > 0) {
                return true;
            }
            return comparedTime == 0 && accountKey.compareTo(oldAccountKey) > 0;
        }
    }
}
