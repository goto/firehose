package com.gotocompany.firehose.config.enums;

/**
 * Declares the format of the message payload that the Firehose filter layer operates on.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.FilterMessageFormatTypeConverter} and read by
 * the JSON and timestamp filters to decide whether the selected key or message bytes must be
 * deserialized from protobuf or interpreted directly as JSON before the filter expression runs.
 *
 * <ul>
 *   <li>{@code JSON} - the payload is treated as JSON.</li>
 *   <li>{@code PROTOBUF} - the payload is a serialized protobuf and is decoded before filtering.</li>
 * </ul>
 */
public enum FilterMessageFormatType {
    JSON, PROTOBUF
}
