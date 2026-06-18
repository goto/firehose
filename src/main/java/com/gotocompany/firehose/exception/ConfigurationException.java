package com.gotocompany.firehose.exception;

/**
 * This exception is thrown when there is invalid configuration encountered.
 */
public class ConfigurationException extends RuntimeException {

    /**
     * Creates the exception with a detail message describing the configuration problem.
     *
     * @param message human-readable description of the invalid configuration
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a detail message and the underlying cause.
     *
     * @param message human-readable description of the invalid configuration
     * @param e       the original exception that surfaced the configuration problem
     */
    public ConfigurationException(String message, Exception e) {
        super(message, e);
    }
}

