package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;

/**
 * KafkaMetadataProtoMessage is a class that contains schema of proto message that contains kafka metadata.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of kafka metadata {@link com.google.protobuf.DynamicMessage}.
 *
 * message KafkaOffsetMetadata{
 *     int64 message_offset = 536870907;
 *     int32 message_partition = 536870908;
 *     string message_topic = 536870909;
 *     Timestamp message_timestamp = 536870910;
 *     Timestamp load_time = 536870911;
 * }
 *
 */
public class KafkaMetadataProtoMessage {
    /** Protobuf message type name for the flat Kafka metadata message. */
    private static final String TYPE_NAME = "KafkaOffsetMetadata";

    /** Field name holding the Kafka record offset. */
    public static final String MESSAGE_OFFSET_FIELD_NAME = "message_offset";
    /** Field name holding the Kafka partition. */
    public static final String MESSAGE_PARTITION_FIELD_NAME = "message_partition";
    /** Field name holding the source Kafka topic. */
    public static final String MESSAGE_TOPIC_FIELD_NAME = "message_topic";
    /** Field name holding the Kafka message timestamp. */
    public static final String MESSAGE_TIMESTAMP_FIELD_NAME = "message_timestamp";
    /** Field name holding the time the record was loaded by Firehose. */
    public static final String LOAD_TIME_FIELD_NAME = "load_time";
    /** Protobuf field number for {@link #MESSAGE_OFFSET_FIELD_NAME}. */
    public static final int MESSAGE_OFFSET_FIELD_NUMBER = 536870907;
    /** Protobuf field number for {@link #MESSAGE_PARTITION_FIELD_NAME}. */
    public static final int MESSAGE_PARTITION_FIELD_NUMBER = 536870908;
    /** Protobuf field number for {@link #MESSAGE_TOPIC_FIELD_NAME}. */
    public static final int MESSAGE_TOPIC_FIELD_NUMBER = 536870909;
    /** Protobuf field number for {@link #MESSAGE_TIMESTAMP_FIELD_NAME}. */
    public static final int MESSAGE_TIMESTAMP_FIELD_NUMBER = 536870910;
    /** Protobuf field number for {@link #LOAD_TIME_FIELD_NAME}. */
    public static final int LOAD_TIME_FIELD_NUMBER = 536870911;

    /**
     * Builds the protobuf {@link com.github.os72.protobuf.dynamic.MessageDefinition} for the
     * {@code KafkaOffsetMetadata} message, declaring its five fields with their types and field
     * numbers.
     *
     * @return the message definition for the Kafka metadata message
     */
    public static MessageDefinition createMessageDefinition() {
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", MESSAGE_OFFSET_FIELD_NAME, MESSAGE_OFFSET_FIELD_NUMBER)
                .addField("optional", "int32", MESSAGE_PARTITION_FIELD_NAME, MESSAGE_PARTITION_FIELD_NUMBER)
                .addField("optional", "string", MESSAGE_TOPIC_FIELD_NAME, MESSAGE_TOPIC_FIELD_NUMBER)
                .addField("optional", "Timestamp", MESSAGE_TIMESTAMP_FIELD_NAME, MESSAGE_TIMESTAMP_FIELD_NUMBER)
                .addField("optional", "Timestamp", LOAD_TIME_FIELD_NAME, LOAD_TIME_FIELD_NUMBER)
                .build();
    }

    /**
     * Returns the protobuf type name of the Kafka metadata message.
     *
     * @return the type name {@code KafkaOffsetMetadata}
     */
    public static String getTypeName() {
        return TYPE_NAME;
    }

    /**
     * Builder of KafkaOffsetMetadata dynamic message.
     */
    public static class MessageBuilder {

        /** Source Kafka topic. */
        private String topic;
        /** Source Kafka partition. */
        private int partition;
        /** Kafka record offset. */
        private long offset;
        /** Time the record was loaded by Firehose. */
        private Instant loadTime;
        /** Timestamp carried by the Kafka message. */
        private Instant messageTimestamp;

        /** Descriptor of the metadata message being built. */
        private Descriptors.Descriptor descriptor;

        /**
         * Creates a builder targeting the given metadata message descriptor.
         *
         * @param descriptor the descriptor of the {@code KafkaOffsetMetadata} message to build
         */
        public MessageBuilder(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        /**
         * Sets the source Kafka topic.
         *
         * @param topic the Kafka topic
         * @return this builder
         */
        public MessageBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Sets the source Kafka partition.
         *
         * @param partition the Kafka partition
         * @return this builder
         */
        public MessageBuilder setPartition(int partition) {
            this.partition = partition;
            return this;
        }

        /**
         * Sets the Kafka record offset.
         *
         * @param offset the Kafka offset
         * @return this builder
         */
        public MessageBuilder setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the time the record was loaded by Firehose.
         *
         * @param loadTime the load time
         * @return this builder
         */
        public MessageBuilder setLoadTime(Instant loadTime) {
            this.loadTime = loadTime;
            return this;
        }

        /**
         * Sets the timestamp carried by the Kafka message.
         *
         * @param messageTimestamp the message timestamp
         * @return this builder
         */
        public MessageBuilder setMessageTimestamp(Instant messageTimestamp) {
            this.messageTimestamp = messageTimestamp;
            return this;
        }

        /**
         * Builds the {@code KafkaOffsetMetadata} dynamic message from the configured values.
         * <p>
         * The {@link Instant} load time and message timestamp are converted to protobuf
         * {@link com.google.protobuf.Timestamp} values via {@link TimestampMetadataProtoMessage}.
         *
         * @return the populated metadata {@link com.google.protobuf.DynamicMessage}
         */
        public DynamicMessage build() {
            Timestamp timestamp = TimestampMetadataProtoMessage.newBuilder()
                    .setSeconds(loadTime.getEpochSecond())
                    .setNanos(loadTime.getNano())
                    .build();
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName(LOAD_TIME_FIELD_NAME), timestamp)
                    .setField(descriptor.findFieldByName(MESSAGE_TIMESTAMP_FIELD_NAME), TimestampMetadataProtoMessage.newBuilder()
                            .setSeconds(messageTimestamp.getEpochSecond())
                            .setNanos(messageTimestamp.getNano())
                            .build())
                    .setField(descriptor.findFieldByName(MESSAGE_OFFSET_FIELD_NAME), offset)
                    .setField(descriptor.findFieldByName(MESSAGE_PARTITION_FIELD_NAME), partition)
                    .setField(descriptor.findFieldByName(MESSAGE_TOPIC_FIELD_NAME), topic)
                    .build();
        }
    }

    /**
     * Creates a {@link MessageBuilder} for the given metadata message descriptor.
     *
     * @param descriptor the descriptor of the {@code KafkaOffsetMetadata} message
     * @return a new builder
     */
    public static MessageBuilder newBuilder(Descriptors.Descriptor descriptor) {
        return new MessageBuilder(descriptor);
    }
}
