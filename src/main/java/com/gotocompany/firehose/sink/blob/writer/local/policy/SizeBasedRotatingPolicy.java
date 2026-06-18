package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

/**
 * {@link WriterPolicy} that rotates a local file once it reaches a maximum size in bytes.
 *
 * @see WriterPolicy
 */
public class SizeBasedRotatingPolicy implements WriterPolicy {

    /** Maximum file size in bytes before rotation is triggered. */
    private final long maxSize;

    /**
     * Creates a size-based rotation policy.
     *
     * @param maxSize the maximum file size in bytes; must be positive
     * @throws IllegalArgumentException if {@code maxSize} is not a positive integer
     */
    public SizeBasedRotatingPolicy(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("The max size should be a positive integer");
        }
        this.maxSize = maxSize;
    }

    /**
     * Determines whether the file has reached the configured maximum size.
     *
     * @param metadata the metadata of the open file
     * @return {@code true} if the file size is at least the maximum, {@code false} otherwise
     */
    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return metadata.getSize() >= maxSize;
    }
}
