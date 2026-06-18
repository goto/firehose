package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.EmptyMessageException;
import com.gotocompany.firehose.exception.UnknownFieldsException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoUtils;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import lombok.AllArgsConstructor;

/**
 * Converts raw Kafka {@link Message} objects into blob sink {@link Record} instances.
 * <p>
 * Parses the message's log payload into a protobuf {@link com.google.protobuf.DynamicMessage} using a
 * Stencil {@link com.gotocompany.stencil.Parser} for the configured input schema, optionally
 * rejecting messages that contain unknown protobuf fields, and attaches the Kafka metadata produced
 * by {@link KafkaMetadataUtils}.
 * <p>
 * Configured by {@link BlobSinkConfig}. The Lombok-generated all-arguments constructor is used mainly
 * for testing, while production code uses {@link #MessageDeSerializer(BlobSinkConfig, StencilClient)}.
 *
 * @see Record
 * @see KafkaMetadataUtils
 */
@AllArgsConstructor
public class MessageDeSerializer {

    /** Descriptor for the generated Kafka metadata proto file, used to build metadata messages. */
    private final Descriptors.FileDescriptor kafkaMetadataFileDescriptor;
    /** Stencil parser that decodes the configured input protobuf schema. */
    private final Parser protoParser;
    /** Blob sink configuration controlling schema class, metadata column and unknown-field handling. */
    private final BlobSinkConfig sinkConfig;

    /**
     * Creates a deserializer for the configured input schema using the given Stencil client.
     *
     * @param sinkConfig the blob sink configuration providing the schema class and metadata settings
     * @param stencilClient the Stencil client used to obtain the protobuf parser
     */
    public MessageDeSerializer(BlobSinkConfig sinkConfig, StencilClient stencilClient) {
        this.sinkConfig = sinkConfig;
        this.protoParser = stencilClient.getParser(sinkConfig.getInputSchemaProtoClass());
        this.kafkaMetadataFileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getOutputKafkaMetadataColumnName());
    }

    /**
     * Deserializes a Kafka message into a {@link Record} with its payload and metadata.
     * <p>
     * Validates that the message has a non-empty log payload, parses it into a protobuf
     * {@link com.google.protobuf.DynamicMessage}, optionally checks for unknown fields, and attaches
     * the Kafka metadata.
     *
     * @param message the Kafka message to deserialize
     * @return the resulting record containing the payload and its Kafka metadata
     * @throws EmptyMessageException if the message has no log payload
     * @throws UnknownFieldsException if unknown protobuf fields are present and not allowed by configuration
     * @throws DeserializerException if the payload cannot be parsed as the configured protobuf schema
     */
    public Record deSerialize(Message message) throws DeserializerException {
        try {
            if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                throw new EmptyMessageException();
            }
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());

            if (!sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
                throw new UnknownFieldsException(dynamicMessage);
            }

            DynamicMessage kafkaMetadata = KafkaMetadataUtils.createKafkaMetadata(kafkaMetadataFileDescriptor, message, sinkConfig.getOutputKafkaMetadataColumnName());
            return new Record(dynamicMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }
}
