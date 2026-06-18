package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration for the sink pool that backs the asynchronous consumer mode.
 *
 * <p>When Firehose runs in {@code ASYNC} mode the consumer factory creates a pool of sinks that
 * process batches concurrently; this interface sizes that pool and tunes how its work queue is
 * polled. Each accessor maps to an environment variable via {@code @Key} and falls back to its
 * {@code @DefaultValue} when unset.
 */
public interface SinkPoolConfig extends AppConfig {
    /**
     * Returns the number of sink worker threads in the asynchronous sink pool, set by
     * {@code SINK_POOL_NUM_THREADS} and defaulting to {@code 1}.
     *
     * @return the sink pool thread count
     */
    @Config.Key("SINK_POOL_NUM_THREADS")
    @Config.DefaultValue("1")
    int getSinkPoolNumThreads();

    /**
     * Returns the timeout in milliseconds used when polling the sink pool's queue for an available
     * sink, set by {@code SINK_POOL_QUEUE_POLL_TIMEOUT_MS} and defaulting to {@code 1000}.
     *
     * @return the sink pool queue poll timeout in milliseconds
     */
    @Config.Key("SINK_POOL_QUEUE_POLL_TIMEOUT_MS")
    @Config.DefaultValue("1000")
    int getSinkPoolQueuePollTimeoutMS();
}
