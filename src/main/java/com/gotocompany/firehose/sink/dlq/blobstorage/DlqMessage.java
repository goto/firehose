package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * JSON representation of a single dead letter queue message stored in blob storage.
 * <p>
 * Serialized by {@link BlobStorageDlqWriter}; the key and value hold the Base64-encoded Kafka record
 * key and payload, alongside the source topic, partition, offset and timestamp and any error details.
 * Lombok generates the all-arguments constructor, getters, setters, {@code equals}, {@code hashCode}
 * and {@code toString}, and each field maps to a JSON property.
 */
@Data
@AllArgsConstructor
public class DlqMessage {
    /** Base64-encoded Kafka record key. */
    @JsonProperty("key")
    private String key;
    /** Base64-encoded Kafka record value. */
    @JsonProperty("value")
    private String value;
    /** Source Kafka topic. */
    @JsonProperty("topic")
    private String topic;
    /** Source Kafka partition. */
    @JsonProperty("partition")
    private int partition;
    /** Kafka record offset. */
    @JsonProperty("offset")
    private long offset;
    /** Message timestamp in epoch milliseconds. */
    @JsonProperty("timestamp")
    private long timestamp;
    /** String form of the error that sent the message to the DLQ. */
    @JsonProperty("error")
    private String error;
    /** Type of the error that sent the message to the DLQ. */
    @JsonProperty("error_type")
    private String errorType;
}
