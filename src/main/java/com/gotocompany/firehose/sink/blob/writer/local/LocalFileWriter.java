package com.gotocompany.firehose.sink.blob.writer.local;

import com.gotocompany.firehose.sink.blob.message.Record;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes blob sink {@link Record} instances to a single local file before it is uploaded to blob
 * storage.
 * <p>
 * Implementations are created per time-partition by {@link LocalStorage} and accumulate records until
 * the file is rotated and closed, at which point its {@link LocalFileMetadata} is finalised. A writer
 * that has been closed rejects further writes.
 *
 * @see LocalParquetFileWriter
 * @see LocalStorage
 */
public interface LocalFileWriter extends Closeable {
    /**
     * @param record to write
     * @return true if write succeeds, false if the writer is closed.
     * @throws IOException if local file writing fails
     */
    boolean write(Record record) throws IOException;

    /**
     * Returns the current metadata for the file, such as its size and record count.
     *
     * @return the file's metadata
     */
    LocalFileMetadata getMetadata();

    /**
     * Closes the file and returns its final metadata.
     *
     * @return the metadata of the closed file
     * @throws IOException if closing the file fails
     */
    LocalFileMetadata closeAndFetchMetaData() throws IOException;

    /**
     * Returns the full local path of the file being written.
     *
     * @return the absolute file path
     */
    String getFullPath();
}
