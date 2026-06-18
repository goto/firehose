package com.gotocompany.firehose.exception;

import java.io.IOException;

/**
 * Thrown when an OAuth2 token exchange fails while authenticating an HTTP sink request.
 *
 * <p>The OAuth2 client raises this when the token endpoint responds with an error payload instead
 * of a valid access token. It extends {@link java.io.IOException} so it integrates naturally with
 * the HTTP client's I/O error handling.
 */
public class OAuth2Exception extends IOException {
    /**
     * Creates the exception with a message describing the OAuth2 failure.
     *
     * @param message detail of the error returned by the token endpoint
     */
    public OAuth2Exception(String message) {
        super(message);
    }
}

