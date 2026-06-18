package com.gotocompany.firehose.config.enums;

/**
 * Declares the wire format of the Kafka messages consumed by the Elasticsearch sink.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.EsSinkMessageTypeConverter} and read by the
 * Elasticsearch request handlers to decide whether each payload must be parsed as a serialized
 * protobuf or treated as raw JSON before it is indexed.
 *
 * <ul>
 *   <li>{@code JSON} - the message body is already JSON and is indexed as supplied.</li>
 *   <li>{@code PROTOBUF} - the message body is a serialized protobuf and is converted to JSON first.</li>
 * </ul>
 */
public enum EsSinkMessageType {
    JSON, PROTOBUF
}
