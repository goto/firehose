package com.gotocompany.firehose.config.enums;

/**
 * Enumerates the HTTP verbs the HTTP sink can use when sending messages to its endpoint.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.HttpSinkRequestMethodConverter} and consumed by
 * the HTTP request factory and request builders. Any value other than {@code POST} or {@code PATCH}
 * is treated as {@code PUT} by the request factory, and {@code DELETE} requests only carry a body
 * when the delete-body option is enabled.
 */
public enum HttpSinkRequestMethodType {
    /**
     * Sends an HTTP PUT request.
     */
    PUT,
    /**
     * Sends an HTTP POST request.
     */
    POST,
    /**
     * Sends an HTTP PATCH request.
     */
    PATCH,
    /**
     * Sends an HTTP DELETE request; a request body is included only when delete-body support is
     * enabled in the HTTP sink configuration.
     */
    DELETE
}
