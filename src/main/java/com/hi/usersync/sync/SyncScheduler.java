package com.hi.usersync.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SyncScheduler {
    private static final Logger log = LoggerFactory.getLogger(SyncScheduler.class);

    private final SyncSourceConfigRepository configRepository;
    private final SyncExecutor syncExecutor;
    private final SyncSchedulerProperties properties;
    private final TaskExecutor syncTaskExecutor;

    public SyncScheduler(SyncSourceConfigRepository configRepository,
                         SyncExecutor syncExecutor,
                         SyncSchedulerProperties properties,
                         @Qualifier("syncTaskExecutor") TaskExecutor syncTaskExecutor) {
        this.configRepository = configRepository;
        this.syncExecutor = syncExecutor;
        this.properties = properties;
        this.syncTaskExecutor = syncTaskExecutor;
    }

    /**
     * This dispatcher runs frequently, but each source has its own persisted next_run_at value.
     * A source is submitted only when its configured N-minute interval is due.
     */
    @Scheduled(fixedDelayString = "${sync.scheduler.dispatch-delay-ms:10000}")
    public void dispatchDueSources() {
        if (!properties.isEnabled()) {
            return;
        }

        for (Long sourceId : configRepository.findDueSourceIds(properties.getBatchSize())) {
            if (!configRepository.claimScheduledRun(sourceId)) {
                continue;
            }

            try {
                syncTaskExecutor.execute(() -> executeScheduled(sourceId));
            } catch (RuntimeException e) {
                configRepository.postponeAfterDispatchFailure(sourceId);
                log.error("Failed to submit scheduled sync task. sourceId={}", sourceId, e);
            }
        }
    }

    private void executeScheduled(Long sourceId) {
        try {
            syncExecutor.runScheduled(sourceId);
        } catch (IllegalStateException e) {
            if (isExpectedSkip(e)) {
                // A concurrent OFF/disable change or an already-running source is not a job failure.
                log.warn("Scheduled sync skipped. sourceId={}, reason={}", sourceId, e.getMessage());
                return;
            }
            log.error("Scheduled sync failed. sourceId={}", sourceId, e);
        } catch (Exception e) {
            log.error("Scheduled sync failed. sourceId={}", sourceId, e);
        }
    }

    private boolean isExpectedSkip(IllegalStateException exception) {
        String message = exception.getMessage();
        return message != null && (
                message.startsWith("Disabled sync schedule:")
                        || message.startsWith("Disabled sync source:")
                        || message.startsWith("The sync source is already running:")
        );
    }
}
