package com.gotocompany.firehose.sink.blob.writer.remote;

/**
 * Unchecked exception thrown when uploading a local file to blob storage fails.
 * <p>
 * Wraps the underlying cause (for example an interruption or a blob storage error) so the
 * upload-tracking code can propagate failures without a checked exception.
 */
public class BlobStorageFailedException extends RuntimeException {
    /**
     * Creates the exception wrapping the given cause.
     *
     * @param th the underlying cause of the upload failure
     */
    public BlobStorageFailedException(Throwable th) {
        super(th);
    }
}
