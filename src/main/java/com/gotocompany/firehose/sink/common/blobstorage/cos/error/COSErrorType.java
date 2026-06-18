package com.gotocompany.firehose.sink.common.blobstorage.cos.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Tencent COS error responses to a named error type by HTTP status code.
 *
 * <p>{@link #fromCode(int)} looks up the constant for a status code, falling back to {@link #DEFAULT_ERROR}
 * when the code is unknown. The name is attached to a
 * {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException} by
 * {@link com.gotocompany.firehose.sink.common.blobstorage.cos.service.TencentObjectOperations}.
 */
public enum COSErrorType {
    /** HTTP 400 Bad Request. */
    BAD_REQUEST(400),
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
    /** HTTP 429 Too Many Requests. */
    TOO_MANY_REQUESTS(429),
    /** HTTP 500 Internal Server Error. */
    INTERNAL_SERVER_ERROR(500),
    /** HTTP 503 Service Unavailable. */
    SERVICE_UNAVAILABLE(503),
    /** HTTP 504 Gateway Timeout. */
    GATEWAY_TIMEOUT(504),
    /** Fallback used when the status code is unknown; carries HTTP 500. */
    DEFAULT_ERROR(500),
    /** HTTP 408 Request Timeout. */
    REQUEST_TIMEOUT(408),
    /** HTTP 411 Length Required. */
    LENGTH_REQUIRED(411),
    /** HTTP 412 Precondition Failed. */
    PRECONDITION_FAILED(412),
    /** HTTP 413 Payload Too Large. */
    PAYLOAD_TOO_LARGE(413),
    /** HTTP 416 Requested Range Not Satisfiable. */
    REQUESTED_RANGE_NOT_SATISFIABLE(416),
    /** HTTP 502 Bad Gateway. */
    BAD_GATEWAY(502);

    /** Lookup table from HTTP status code to error type. */
    private static final Map<Integer, COSErrorType> ERROR_CODE_MAP = new HashMap<>();

    static {
        for (COSErrorType errorType : values()) {
            ERROR_CODE_MAP.put(errorType.code, errorType);
        }
    }

    /** The HTTP status code represented by this constant. */
    private final int code;

    /**
     * Creates an error type bound to an HTTP status code.
     *
     * @param code the HTTP status code
     */
    COSErrorType(int code) {
        this.code = code;
    }

    /**
     * Returns the error type for an HTTP status code.
     *
     * @param code the HTTP status code returned by the COS client
     * @return the matching error type, or {@link #DEFAULT_ERROR} when the code is unknown
     */
    public static COSErrorType fromCode(int code) {
        return ERROR_CODE_MAP.getOrDefault(code, DEFAULT_ERROR);
    }

    /**
     * Returns the HTTP status code represented by this error type.
     *
     * @return the HTTP status code
     */
    public int getCode() {
        return code;
    }
}
