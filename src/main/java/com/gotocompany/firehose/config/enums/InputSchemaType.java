package com.gotocompany.firehose.config.enums;

/**
 * Declares the encoding of the messages that Firehose reads from the input Kafka topic.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.InputSchemaTypeConverter} and exposed through
 * {@link com.gotocompany.firehose.config.AppConfig#getInputSchemaType()}. It drives how raw Kafka
 * bytes are deserialized before they are parsed, filtered and forwarded to a sink.
 */
public enum InputSchemaType {
    /**
     * Input messages are encoded as protobuf and are deserialized using the configured proto schema.
     */
    PROTOBUF,
    /**
     * Input messages are encoded as JSON.
     */
    JSON
}
