package com.gotocompany.firehose.sink.common.blobstorage;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Should be thrown when there is exception thrown by blob storage client.
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class BlobStorageException extends Exception {
    /** Provider-specific classification of the failure, such as an error code or status name. */
    private final String errorType;
    /** Human-readable description of the failure. */
    private final String message;

    /**
     * Creates a blob storage exception.
     *
     * @param errorType the provider-specific error classification
     * @param message   the failure description
     * @param cause     the underlying cause
     */
    public BlobStorageException(String errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.message = message;
    }
}
