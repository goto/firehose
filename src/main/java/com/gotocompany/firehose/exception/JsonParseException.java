package com.gotocompany.firehose.exception;

/**
 * Unchecked exception raised when a string that is expected to be valid JSON cannot be parsed.
 *
 * <p>Sink request handlers that build payloads from JSON templates (for example the Elasticsearch
 * and MongoDB request handlers) throw this to convert a low-level parsing failure into a
 * Firehose-specific error while retaining the original cause.
 */
public class JsonParseException extends RuntimeException {
    /**
     * Creates the exception with a human-readable message and the underlying parsing failure.
     *
     * @param message description of what could not be parsed
     * @param cause   the original error thrown by the JSON parser
     */
    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
