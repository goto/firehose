package com.gotocompany.firehose.config.enums;

/**
 * Selects where the HTTP sink places the dynamic parameters extracted from a message.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.HttpSinkParameterPlacementTypeConverter} and is
 * evaluated alongside {@link HttpSinkParameterSourceType} to decide whether a request is built with
 * query-string parameters or with HTTP headers.
 */
public enum HttpSinkParameterPlacementType {
    /**
     * Append the parameters to the request URL as query-string entries.
     */
    QUERY,
    /**
     * Add the parameters to the request as HTTP headers.
     */
    HEADER
}
