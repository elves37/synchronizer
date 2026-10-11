package com.hi.usersync.api;

import com.hi.usersync.sync.SyncExecutor;
import com.hi.usersync.sync.SyncRunSummary;
import com.hi.usersync.sync.SyncSchedulerProperties;
import com.hi.usersync.sync.SyncSourceConfig;
import com.hi.usersync.sync.SyncSourceConfigRepository;
import com.hi.usersync.sync.SyncSqlValidator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/sync-sources")
public class SyncSourceController {
    private final SyncSourceConfigRepository repository;
    private final SyncExecutor syncExecutor;
    private final SyncSqlValidator sqlValidator;
    private final SyncSchedulerProperties schedulerProperties;

    public SyncSourceController(SyncSourceConfigRepository repository,
                                SyncExecutor syncExecutor,
                                SyncSqlValidator sqlValidator,
                                SyncSchedulerProperties schedulerProperties) {
        this.repository = repository;
        this.syncExecutor = syncExecutor;
        this.sqlValidator = sqlValidator;
        this.schedulerProperties = schedulerProperties;
    }

    @GetMapping
    public List<SyncSourceConfig> list() {
        return repository.findAll();
    }

    @GetMapping("/scheduler-status")
    public SchedulerStatus schedulerStatus() {
        return new SchedulerStatus(
                schedulerProperties.isEnabled(),
                schedulerProperties.getDispatchDelayMs(),
                schedulerProperties.getBatchSize(),
                schedulerProperties.getPoolSize()
        );
    }

    @GetMapping("/{id}")
    public SyncSourceConfig get(@PathVariable Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Unknown source id: " + id));
    }

    @PostMapping
    public SyncSourceConfig create(@Valid @RequestBody SyncSourceRequest request) {
        sqlValidator.validateFetchSql(request.incrementalSql());
        sqlValidator.validateFetchOneSql(request.fetchOneSql());
        Long id = repository.create(request.toConfig(null));
        return get(id);
    }

    @PutMapping("/{id}")
    public SyncSourceConfig update(@PathVariable Long id,
                                   @Valid @RequestBody SyncSourceRequest request) {
        sqlValidator.validateFetchSql(request.incrementalSql());
        sqlValidator.validateFetchOneSql(request.fetchOneSql());
        repository.update(id, request.toConfig(id));
        return get(id);
    }

    /**
     * Enables/disables automatic synchronization and resets next_run_at according to the supplied
     * N-minute interval. Manual execution remains available while the automatic schedule is OFF.
     */
    @PutMapping("/{id}/schedule")
    public SyncSourceConfig updateSchedule(@PathVariable Long id,
                                           @Valid @RequestBody ScheduleRequest request) {
        repository.updateSchedule(id, request.enabled(), request.intervalMinutes());
        return get(id);
    }

    @PostMapping("/{id}/run")
    public SyncRunSummary run(@PathVariable Long id) {
        return syncExecutor.run(id);
    }

    public record SchedulerStatus(
            boolean enabled,
            long dispatchDelayMs,
            int batchSize,
            int poolSize
    ) {
    }

    public record ScheduleRequest(
            @NotNull Boolean enabled,
            @NotNull @Min(1) @Max(10080) Integer intervalMinutes
    ) {
    }

    public record SyncSourceRequest(
            @NotBlank String sourceDbKey,
            @NotBlank String name,
            @NotBlank String incrementalSql,
            String fetchOneSql,
            String keyAlias,
            String updatedAtAlias,
            @NotBlank String upsertSql,
            @Min(1) Integer chunkSize,
            @Min(1) Integer fetchSize,
            Integer safetyLagSeconds,
            Integer overlapSeconds,
            Boolean enabled,
            Boolean scheduleEnabled,
            @Min(1) @Max(10080) Integer scheduleIntervalMinutes
    ) {
        SyncSourceConfig toConfig(Long id) {
            return new SyncSourceConfig(
                    id,
                    sourceDbKey,
                    name,
                    incrementalSql,
                    fetchOneSql,
                    keyAlias == null || keyAlias.isBlank() ? "account_key" : keyAlias,
                    updatedAtAlias == null || updatedAtAlias.isBlank() ? "sync_updated_at" : updatedAtAlias,
                    upsertSql,
                    chunkSize == null ? 500 : chunkSize,
                    fetchSize == null ? 500 : fetchSize,
                    safetyLagSeconds == null ? 60 : Math.max(safetyLagSeconds, 0),
                    overlapSeconds == null ? 300 : Math.max(overlapSeconds, 0),
                    enabled == null || enabled,
                    scheduleEnabled != null && scheduleEnabled,
                    scheduleIntervalMinutes == null ? 5 : scheduleIntervalMinutes,
                    null,
                    null,
                    null,
                    null
            );
        }
    }
}
