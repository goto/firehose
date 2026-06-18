package com.gotocompany.firehose.exception;

/**
 * Empty thrown when the message is contains zero bytes.
 */
public class EmptyMessageException extends DeserializerException {
    /**
     * Creates the exception with the fixed detail message {@code "log message is empty"}.
     */
    public EmptyMessageException() {
        super("log message is empty");
    }
}
