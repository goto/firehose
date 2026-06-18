package com.gotocompany.firehose.sink.blob.writer.local;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Immutable metadata describing a single local file staged by the blob sink.
 * <p>
 * Captures the file's base and full paths, its creation time, the number of records it holds and its
 * size in bytes. Used by rotation policies to decide when to rotate a writer and by the upload stage
 * to locate the file. Lombok generates the all-arguments constructor, getters, {@code equals} and
 * {@code hashCode}.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class LocalFileMetadata {
    /** Base local directory the file lives under. */
    private final String basePath;
    /** Full path of the local file. */
    private final String fullPath;
    /** Epoch milliseconds at which the file was created. */
    private final long createdTimestampMillis;
    /** Number of records written to the file. */
    private final long recordCount;
    /** Current size of the file in bytes. */
    private final long size;
}
