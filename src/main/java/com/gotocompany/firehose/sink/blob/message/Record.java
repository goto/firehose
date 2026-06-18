package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

/**
 * A single blob sink record: the deserialized message payload together with its Kafka metadata.
 * <p>
 * Produced by {@link MessageDeSerializer} and consumed by the writer pipeline. The payload and
 * metadata are held as protobuf {@link com.google.protobuf.DynamicMessage} instances; helper methods
 * extract the values needed to choose an output partition, namely the source Kafka topic and the
 * record timestamp.
 * <p>
 * Lombok generates the all-arguments constructor, getters, setters, {@code equals}, {@code hashCode}
 * and {@code toString}.
 */
@AllArgsConstructor
@Data
public class Record {
    /** The deserialized message payload. */
    private DynamicMessage message;
    /** The Kafka metadata (topic, partition, offset and timestamps) associated with the payload. */
    private DynamicMessage metadata;

    /**
     * Returns the source Kafka topic recorded in this record's metadata.
     * <p>
     * When {@code fieldName} is non-empty the metadata is nested under that field and the topic is
     * read from the nested message; otherwise it is read directly from the top-level metadata.
     *
     * @param fieldName the name of the nesting metadata field, or empty if the metadata is not nested
     * @return the Kafka topic the message was consumed from
     */
    public String getTopic(String fieldName) {
        Descriptors.Descriptor metadataDescriptor = metadata.getDescriptorForType();

        if (!fieldName.isEmpty()) {
            DynamicMessage nestedMetadataMessage = (DynamicMessage) metadata.getField(metadataDescriptor.findFieldByName(fieldName));
            Descriptors.Descriptor nestedMetadataMessageDescriptor = nestedMetadataMessage.getDescriptorForType();
            return (String) nestedMetadataMessage.getField(nestedMetadataMessageDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
        }

        return (String) metadata.getField(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
    }

    /**
     * Extracts a timestamp from the message payload as an {@link Instant}.
     * <p>
     * Reads the protobuf {@code Timestamp} field with the given name and converts its {@code seconds}
     * and {@code nanos} components into an instant. Used to derive the time partition for the record.
     *
     * @param fieldName the name of the protobuf timestamp field within the payload
     * @return the timestamp value as an {@link Instant}
     */
    public Instant getTimestamp(String fieldName) {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName(fieldName);
        DynamicMessage timestamp = (DynamicMessage) message.getField(timestampField);
        long seconds = (long) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (int) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
