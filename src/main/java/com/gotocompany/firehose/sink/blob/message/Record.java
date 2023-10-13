package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@AllArgsConstructor
@Data
public class Record {
    private DynamicMessage message;
    private DynamicMessage metadata;

    public String getTopic(String fieldName) {
        Descriptors.Descriptor metadataDescriptor = metadata.getDescriptorForType();

        if (!fieldName.isEmpty()) {
            DynamicMessage nestedMetadataMessage = (DynamicMessage) metadata.getField(metadataDescriptor.findFieldByName(fieldName));
            Descriptors.Descriptor nestedMetadataMessageDescriptor = nestedMetadataMessage.getDescriptorForType();
            return (String) nestedMetadataMessage.getField(nestedMetadataMessageDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
        }

        return (String) metadata.getField(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
    }

    public Instant getTimestampFromMessage(String fieldName) {
        return getTimeStampFromDescriptor(fieldName, message);
    }

    public Instant getTimestampFromMetadata(String fieldName) {
        return getTimeStampFromDescriptor(fieldName, metadata);
    }

    public Instant getTimeStampFromDescriptor(String fieldName, DynamicMessage m) {
        Descriptors.Descriptor descriptor = m.getDescriptorForType();
        Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName(fieldName);
        DynamicMessage timestamp = (DynamicMessage) m.getField(timestampField);
        long seconds = (long) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (int) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }

    public LocalDateTime getLocalDateTime(BlobSinkConfig config) {
        switch (config.getFilePartitionTimeType()) {
            case MESSAGE_TIMESTAMP:
                return LocalDateTime.ofInstant(
                        getTimestampFromMetadata(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME),
                        ZoneId.of(config.getFilePartitionProtoTimestampTimezone()));
            case PROCESSING_TIMESTAMP:
                return LocalDateTime.now();
            default:
                return LocalDateTime.ofInstant(
                        getTimestampFromMessage(config.getFilePartitionProtoTimestampFieldName()),
                        ZoneId.of(config.getFilePartitionProtoTimestampTimezone()));

        }
    }
}
