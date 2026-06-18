package com.gotocompany.firehose.config.enums;

/**
 * Declares the wire format of the Kafka messages consumed by the MongoDB sink.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.MongoSinkMessageTypeConverter} and read by the
 * MongoDB request handler to decide whether each payload must be parsed as a serialized protobuf or
 * treated as raw JSON before it is written as a document.
 *
 * <ul>
 *   <li>{@code JSON} - the message body is already JSON and is stored as supplied.</li>
 *   <li>{@code PROTOBUF} - the message body is a serialized protobuf and is converted to JSON first.</li>
 * </ul>
 */
public enum MongoSinkMessageType {
    JSON, PROTOBUF
}
