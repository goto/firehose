package com.gotocompany.firehose.sink.common.blobstorage.gcs.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Error types from exception thrown by google cloud storage client.
 * There might be newer error codes that thrown by gcs client, need to update this list of error.
 */
public enum GCSErrorType {
    /** HTTP 302 Found (redirection). */
    FOUND(302),
    /** HTTP 303 See Other. */
    SEE_OTHER(303),
    /** HTTP 304 Not Modified. */
    NOT_MODIFIED(304),
    /** HTTP 305 Use Proxy / temporary redirect. */
    TEMPORARY_REDIRECT(305),
    /** HTTP 308 Resume Incomplete, used by resumable uploads. */
    RESUME_INCOMPLETE(308),
    /** HTTP 401 Unauthorized. */
    UNAUTHORIZED(401),
    /** HTTP 403 Forbidden. */
    FORBIDDEN(403),
    /** HTTP 404 Not Found. */
    NOT_FOUND(404),
    /** HTTP 405 Method Not Allowed. */
    METHOD_NOT_ALLOWED(405),
    /** HTTP 409 Conflict. */
    CONFLICT(409),
    /** HTTP 410 Gone. */
    GONE(410),
    /** HTTP 411 Length Required. */
    LENGTH_REQUIRED(411),
    /** HTTP 412 Precondition Failed. */
    PRECONDITION_FAILED(412),
    /** HTTP 413 Payload Too Large. */
    PAYLOAD_TOO_LARGE(413),
    /** HTTP 416 Requested Range Not Satisfiable. */
    REQUESTED_RANGE_NOT_SATISFIABLE(416),
    /** HTTP 499 Client Closed Request. */
    CLIENT_CLOSED_REQUEST(499),
    /** HTTP 400 Bad Request. */
    BAD_REQUEST(400),
    /** HTTP 504 Gateway Timeout. */
    GATEWAY_TIMEOUT(504),
    /** HTTP 503 Service Unavailable. */
    SERVICE_UNAVAILABLE(503),
    /** HTTP 502 Bad Gateway. */
    BAD_GATEWAY(502),
    /** HTTP 500 Internal Server Error. */
    INTERNAL_SERVER_ERROR(500),
    /** HTTP 429 Too Many Requests. */
    TOO_MANY_REQUEST(429),
    /** HTTP 408 Request Timeout. */
    REQUEST_TIMEOUT(408),
    /** Fallback used when the status code is unknown; carries no HTTP code. */
    DEFAULT_ERROR;

    /** Lookup table from HTTP status code to error type, populated for constants that carry a code. */
    private static final Map<Integer, GCSErrorType> ERROR_NUMBER_TYPE_MAP = new HashMap<>();

    static {
        for (GCSErrorType errorType : values()) {
            ERROR_NUMBER_TYPE_MAP.put(errorType.codeValue, errorType);
        }
    }

    /**
     * Returns the error type for an HTTP status code.
     *
     * @param code the HTTP status code returned by the GCS client
     * @return the matching error type, or {@link #DEFAULT_ERROR} when the code is unknown
     */
    public static GCSErrorType valueOfCode(int code) {
        return ERROR_NUMBER_TYPE_MAP.getOrDefault(code, DEFAULT_ERROR);
    }

    /** The HTTP status code represented by this constant, or zero for {@link #DEFAULT_ERROR}. */
    private int codeValue;

    /**
     * Creates an error type bound to an HTTP status code.
     *
     * @param codeValue the HTTP status code
     */
    GCSErrorType(int codeValue) {
        this.codeValue = codeValue;
    }

    /**
     * Creates an error type with no associated status code, used by {@link #DEFAULT_ERROR}.
     */
    GCSErrorType() {

    }
}
