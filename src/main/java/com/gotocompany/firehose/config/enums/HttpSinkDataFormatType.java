package com.gotocompany.firehose.config.enums;

/**
 * Selects how the HTTP sink serializes a message into the body of an outgoing request.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.HttpSinkDataFormatTypeConverter} and read by the
 * HTTP serializer factory. When the proto schema is empty the sink falls back to the proto-bytes
 * serializer regardless of this setting.
 */
public enum HttpSinkDataFormatType {
    /**
     * Send the raw serialized protobuf bytes as the request body.
     */
    PROTO,
    /**
     * Convert the message to JSON and send it as the request body.
     */
    JSON
}
