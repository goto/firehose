package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

/**
 * {@link WriterPolicy} that rotates a local file once it has been open for a maximum duration.
 *
 * @see WriterPolicy
 */
public class TimeBasedRotatingPolicy implements WriterPolicy {

    /** Maximum time in milliseconds a file may stay open before rotation. */
    private final long maxRotatingDurationMillis;

    /**
     * Creates a time-based rotation policy.
     *
     * @param maxRotatingDurationMillis the maximum open duration in milliseconds; must be positive
     * @throws IllegalArgumentException if {@code maxRotatingDurationMillis} is not a positive integer
     */
    public TimeBasedRotatingPolicy(long maxRotatingDurationMillis) {
        if (maxRotatingDurationMillis <= 0) {
            throw new IllegalArgumentException("The max duration should be a positive integer");
        }
        this.maxRotatingDurationMillis = maxRotatingDurationMillis;
    }

    /**
     * Determines whether the file has been open longer than the configured duration.
     *
     * @param metadata the metadata of the open file
     * @return {@code true} if the file's age is at least the maximum duration, {@code false} otherwise
     */
    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return System.currentTimeMillis() - metadata.getCreatedTimestampMillis() >= maxRotatingDurationMillis;
    }
}
