package com.hi.usersync.sync;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "sync.scheduler")
public class SyncSchedulerProperties {
    private boolean enabled = true;
    private long dispatchDelayMs = 10_000L;
    private int batchSize = 100;
    private int poolSize = 4;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getDispatchDelayMs() {
        return dispatchDelayMs;
    }

    public void setDispatchDelayMs(long dispatchDelayMs) {
        this.dispatchDelayMs = Math.max(dispatchDelayMs, 1_000L);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = Math.max(batchSize, 1);
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = Math.max(poolSize, 1);
    }
}
