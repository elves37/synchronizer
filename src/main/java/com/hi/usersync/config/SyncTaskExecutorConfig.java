package com.hi.usersync.config;

import com.hi.usersync.sync.SyncSchedulerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration(proxyBeanMethods = false)
public class SyncTaskExecutorConfig {

    @Bean(name = "syncTaskExecutor")
    public ThreadPoolTaskExecutor syncTaskExecutor(SyncSchedulerProperties properties) {
        int poolSize = Math.max(properties.getPoolSize(), 1);

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        // Do not build a long queue. A rejected dispatch is persisted for a short retry instead,
        // which prevents the same source from accumulating multiple queued schedule executions.
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("user-sync-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }
}
