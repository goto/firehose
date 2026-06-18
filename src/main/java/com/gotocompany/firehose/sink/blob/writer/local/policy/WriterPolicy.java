package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

/**
 * Strategy that decides whether an open local file should be rotated, that is closed and uploaded.
 * <p>
 * Implementations inspect a file's {@link LocalFileMetadata} and are evaluated by
 * {@link com.gotocompany.firehose.sink.blob.writer.local.LocalStorage}; a file is rotated as soon as
 * any configured policy matches.
 *
 * @see SizeBasedRotatingPolicy
 * @see TimeBasedRotatingPolicy
 */
public interface WriterPolicy {
    /**
     * Determines whether the file described by the given metadata should be rotated.
     *
     * @param metadata the metadata of the open file
     * @return {@code true} if the file should be rotated, {@code false} otherwise
     */
    boolean shouldRotate(LocalFileMetadata metadata);
}
