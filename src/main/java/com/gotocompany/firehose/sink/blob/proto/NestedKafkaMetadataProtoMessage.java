package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;


/**
 * NestedKafkaMetadataProtoMessage contains schema of kafka metadata proto message nested under a top level field.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of kafka metadata {@link com.google.protobuf.DynamicMessage}.
 * message KafkaNestedOffsetMetadata{
 *     KafkaOffsetMetadata ${kafka_metadata_column_name} = 536870911;
 * }
 *
 */
@AllArgsConstructor
public class NestedKafkaMetadataProtoMessage {
    /** Protobuf message type name for the nested Kafka metadata message. */
    private static final String NESTED_OFFSET_METADATA_PROTO_NAME = "KafkaNestedOffsetMetadata";
    /** Protobuf field number of the nested metadata column. */
    public static final int METADATA_FIELD_NUMBER = 536870911;

    /**
     * Returns the protobuf type name of the nested Kafka metadata message.
     *
     * @return the type name {@code KafkaNestedOffsetMetadata}
     */
    public static String getTypeName() {
        return NESTED_OFFSET_METADATA_PROTO_NAME;
    }

    /**
     * Builds the protobuf {@link com.github.os72.protobuf.dynamic.MessageDefinition} for the nested
     * metadata message.
     * <p>
     * Embeds the supplied flat metadata definition and declares a single field of that type named
     * after the configured metadata column.
     *
     * @param nestedKafkaMetadataColumnName the column name under which the metadata is nested
     * @param kafkaMetadataProtoTypeName the protobuf type name of the embedded metadata message
     * @param metadataMessageDefinition the definition of the flat metadata message to embed
     * @return the message definition for the nested metadata message
     */
    public static MessageDefinition createMessageDefinition(String nestedKafkaMetadataColumnName, String kafkaMetadataProtoTypeName, MessageDefinition metadataMessageDefinition) {

        return MessageDefinition.newBuilder(NestedKafkaMetadataProtoMessage.getTypeName())
                .addMessageDefinition(metadataMessageDefinition)
                .addField("optional", kafkaMetadataProtoTypeName, nestedKafkaMetadataColumnName, METADATA_FIELD_NUMBER)
                .build();
    }

    /**
     * Creates a {@link MessageBuilder} for the given nested metadata message descriptor.
     *
     * @param descriptor the descriptor of the {@code KafkaNestedOffsetMetadata} message
     * @return a new builder
     */
    public static MessageBuilder newMessageBuilder(Descriptors.Descriptor descriptor) {
        return new MessageBuilder(descriptor);
    }

    /**
     * Builder of KafkaNestedOffsetMetadata dynamic message.
     */
    public static class MessageBuilder {

        /** Name of the field under which the metadata is nested. */
        private String metadataColumnName;
        /** The flat Kafka metadata message to embed. */
        private DynamicMessage metadata;

        /** Descriptor of the nested metadata message being built. */
        private Descriptors.Descriptor descriptor;

        /**
         * Creates a builder targeting the given nested metadata message descriptor.
         *
         * @param descriptor the descriptor of the {@code KafkaNestedOffsetMetadata} message to build
         */
        public MessageBuilder(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        /**
         * Sets the field name under which the metadata is nested.
         *
         * @param metadataColumnName the nesting column name
         * @return this builder
         */
        public MessageBuilder setMetadataColumnName(String metadataColumnName) {
            this.metadataColumnName = metadataColumnName;
            return this;
        }

        /**
         * Sets the flat metadata message to embed.
         *
         * @param metadata the Kafka metadata message
         * @return this builder
         */
        public MessageBuilder setMetadata(DynamicMessage metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Builds the nested metadata dynamic message, placing the embedded metadata under the
         * configured column.
         *
         * @return the populated nested metadata {@link com.google.protobuf.DynamicMessage}
         */
        public DynamicMessage build() {
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName(metadataColumnName), metadata)
                    .build();
        }
    }
}
