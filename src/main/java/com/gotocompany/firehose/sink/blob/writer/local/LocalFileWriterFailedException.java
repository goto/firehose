package com.gotocompany.firehose.sink.blob.writer.local;

import java.io.IOException;

/**
 * Unchecked exception thrown when creating, writing to, closing or deleting a local blob sink file
 * fails.
 * <p>
 * Wraps the underlying {@link IOException} so the blob sink's write pipeline can surface local
 * filesystem failures without a checked exception.
 */
public class LocalFileWriterFailedException extends RuntimeException {
    /**
     * Creates the exception wrapping the given I/O failure.
     *
     * @param e the underlying I/O exception
     */
    public LocalFileWriterFailedException(IOException e) {
        super(e);
    }
}
