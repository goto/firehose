package com.gotocompany.firehose.sink.common.blobstorage.cos.error;

import java.util.HashMap;
import java.util.Map;

public enum COSErrorType {
    BAD_REQUEST(400),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    NOT_FOUND(404),
    METHOD_NOT_ALLOWED(405),
    CONFLICT(409),
    TOO_MANY_REQUESTS(429),
    INTERNAL_SERVER_ERROR(500),
    SERVICE_UNAVAILABLE(503),
    GATEWAY_TIMEOUT(504),
    DEFAULT_ERROR(500),
    REQUEST_TIMEOUT(408),
    LENGTH_REQUIRED(411),
    PRECONDITION_FAILED(412),
    PAYLOAD_TOO_LARGE(413),
    REQUESTED_RANGE_NOT_SATISFIABLE(416),
    BAD_GATEWAY(502);

    private static final Map<Integer, COSErrorType> ERROR_CODE_MAP = new HashMap<>();

    static {
        for (COSErrorType errorType : values()) {
            ERROR_CODE_MAP.put(errorType.code, errorType);
        }
    }

    private final int code;

    COSErrorType(int code) {
        this.code = code;
    }

    public static COSErrorType fromCode(int code) {
        return ERROR_CODE_MAP.getOrDefault(code, DEFAULT_ERROR);
    }

    public int getCode() {
        return code;
    }
} 