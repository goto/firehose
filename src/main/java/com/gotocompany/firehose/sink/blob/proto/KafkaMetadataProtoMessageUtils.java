package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import lombok.Getter;


/**
 * KafkaMetadataProtoUtils provide function to create {@link com.google.protobuf.Descriptors.FileDescriptor FileDescriptor} of kafka metadata proto message.
 */
@Getter
public class KafkaMetadataProtoMessageUtils {
    /** Name of the generated protobuf file. */
    public static final String FILE_NAME = "Metadata.proto";
    /** Protobuf package of the generated schema. */
    public static final String PACKAGE = "google.protobuf";

    /**
     * Builds the metadata file descriptor for the given metadata column configuration.
     *
     * @param kafkaMetadataColumnName the metadata column name; when non-empty the nested metadata message is included
     * @return the file descriptor describing the Kafka metadata schema
     * @throws IllegalArgumentException if the generated schema is invalid
     */
    public static Descriptors.FileDescriptor createFileDescriptor(String kafkaMetadataColumnName) {
        DynamicSchema schema = createSchema(kafkaMetadataColumnName);
        return createFileDescriptor(schema);
    }

    /**
     * Builds the dynamic protobuf schema for the Kafka metadata.
     * <p>
     * Always includes the timestamp and flat Kafka metadata definitions; when a metadata column name
     * is supplied it also adds the nested metadata definition.
     *
     * @param kafkaMetadataColumnName the metadata column name, or empty for a flat schema
     * @return the assembled dynamic schema
     * @throws IllegalArgumentException if the schema fails protobuf validation
     */
    private static DynamicSchema createSchema(String kafkaMetadataColumnName) {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

        MessageDefinition timestampMessageDefinition = TimestampMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(timestampMessageDefinition);

        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);

        if (!kafkaMetadataColumnName.isEmpty()) {
            MessageDefinition kafkaNestedMetadataProtoMessageDefinition = NestedKafkaMetadataProtoMessage
                    .createMessageDefinition(
                            kafkaMetadataColumnName,
                            KafkaMetadataProtoMessage.getTypeName(),
                            kafkaMetadataMessageDefinition);
            schemaBuilder.addMessageDefinition(kafkaNestedMetadataProtoMessageDefinition);
        }

        DynamicSchema schema;
        try {
            schema = schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid proto schema", e);
        }
        return schema;
    }

    /**
     * Builds a file descriptor from an already-assembled dynamic schema.
     * <p>
     * Uses the first file in the schema's descriptor set and resolves it with no external
     * dependencies.
     *
     * @param schema the dynamic schema to convert
     * @return the resulting file descriptor
     * @throws IllegalArgumentException if the descriptor fails protobuf validation
     */
    public static Descriptors.FileDescriptor createFileDescriptor(DynamicSchema schema) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = schema.getFileDescriptorSet();
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptorSet.getFile(0);
        Descriptors.FileDescriptor[] dependencies = {};

        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid proto schema", e);
        }
    }
}
