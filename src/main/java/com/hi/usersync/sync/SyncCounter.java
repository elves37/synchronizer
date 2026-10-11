package com.hi.usersync.sync;

public class SyncCounter {
    private int totalCount;
    private int upsertedCount;
    private int unchangedCount;
    private int skippedExcludedCount;
    private int failedCount;

    public void incTotal() { totalCount++; }
    public void incUpserted() { upsertedCount++; }
    public void incUnchanged() { unchangedCount++; }
    public void incSkippedExcluded() { skippedExcludedCount++; }
    public void incFailed() { failedCount++; }

    public int totalCount() { return totalCount; }
    public int upsertedCount() { return upsertedCount; }
    public int unchangedCount() { return unchangedCount; }
    public int skippedExcludedCount() { return skippedExcludedCount; }
    public int failedCount() { return failedCount; }
}
