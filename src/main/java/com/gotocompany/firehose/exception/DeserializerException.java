package com.gotocompany.firehose.exception;

/**
 * Deserializer exception is thrown when message from proto is not deserializable into the Java object.
 */
public class DeserializerException extends RuntimeException {

    /**
     * Creates the exception with a detail message describing the deserialization failure.
     *
     * @param message human-readable description of the failure
     */
    public DeserializerException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a detail message and the underlying cause.
     *
     * @param message human-readable description of the failure
     * @param e       the original exception that triggered this failure
     */
    public DeserializerException(String message, Exception e) {
        super(message, e);
    }
}
