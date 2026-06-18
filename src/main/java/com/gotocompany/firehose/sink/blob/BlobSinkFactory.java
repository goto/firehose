package com.gotocompany.firehose.sink.blob;

import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.message.MessageDeSerializer;
import com.gotocompany.firehose.sink.blob.writer.WriterOrchestrator;
import com.gotocompany.firehose.sink.blob.writer.local.LocalStorage;
import com.gotocompany.firehose.sink.blob.writer.local.policy.SizeBasedRotatingPolicy;
import com.gotocompany.firehose.sink.blob.writer.local.policy.TimeBasedRotatingPolicy;
import com.gotocompany.firehose.sink.blob.writer.local.policy.WriterPolicy;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageFactory;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import com.gotocompany.firehose.sink.blob.proto.NestedKafkaMetadataProtoMessage;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory that assembles a {@link BlobSink} and its collaborators from configuration.
 * <p>
 * Builds the {@link BlobSinkConfig} from the supplied settings and wires together the
 * {@link LocalStorage} (with its time- and size-based rotation policies), the remote
 * {@link BlobStorage} backend (GCS, S3, OSS or COS), the {@link WriterOrchestrator} and the
 * {@link MessageDeSerializer}. The protobuf descriptor used for the output records is resolved
 * through the Stencil client, and the Kafka metadata columns are described by a generated metadata
 * proto.
 *
 * @see BlobSink
 * @see WriterOrchestrator
 */
public class BlobSinkFactory {

    /**
     * Builds a fully wired {@link BlobSink} from the given configuration.
     * <p>
     * Creates the blob sink configuration, the local storage with its rotation policies, the remote
     * blob storage backend, the writer orchestrator and the message deserializer, then returns a
     * blob sink composed of them.
     *
     * @param configuration the raw sink configuration key-value pairs
     * @param offsetManager the offset manager used by the sink to track committable offsets
     * @param statsDReporter the reporter used to publish metrics
     * @param stencilClient the Stencil client used to resolve the input protobuf schema
     * @return a ready-to-use blob {@link Sink}
     */
    public static Sink create(Map<String, String> configuration, OffsetManager offsetManager, StatsDReporter statsDReporter, StencilClient stencilClient) {
        BlobSinkConfig sinkConfig = ConfigFactory.create(BlobSinkConfig.class, configuration);
        LocalStorage localStorage = getLocalFileWriterWrapper(sinkConfig, stencilClient, statsDReporter);
        BlobStorage sinkBlobStorage = createSinkObjectStorage(sinkConfig, new HashMap<>(configuration));
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, sinkBlobStorage, statsDReporter);
        MessageDeSerializer messageDeSerializer = new MessageDeSerializer(sinkConfig, stencilClient);
        return new BlobSink(
                new FirehoseInstrumentation(statsDReporter, BlobSink.class),
                sinkConfig.getSinkType().toString(),
                offsetManager,
                writerOrchestrator,
                messageDeSerializer);
    }

    /**
     * Resolves the protobuf descriptor that describes the Kafka metadata columns.
     * <p>
     * When no metadata column name is configured the flat {@link KafkaMetadataProtoMessage} type is
     * used; otherwise the {@link NestedKafkaMetadataProtoMessage} type, which nests the metadata under
     * the configured column, is used.
     *
     * @param sinkConfig the blob sink configuration
     * @return the descriptor for the metadata message matching the configuration
     */
    private static Descriptors.Descriptor getMetadataMessageDescriptor(BlobSinkConfig sinkConfig) {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getOutputKafkaMetadataColumnName());
        return sinkConfig.getOutputKafkaMetadataColumnName().isEmpty()
                ? fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName())
                : fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());
    }

    /**
     * Builds the {@link LocalStorage} that writes records to local Parquet files.
     * <p>
     * Resolves the output message descriptor from the Stencil client, combines it with the metadata
     * descriptor's fields, and configures time-based and size-based file rotation policies from the
     * sink configuration.
     *
     * @param sinkConfig the blob sink configuration
     * @param stencilClient the Stencil client used to resolve the output protobuf schema
     * @param statsDReporter the reporter used to publish metrics
     * @return the configured local storage
     */
    private static LocalStorage getLocalFileWriterWrapper(BlobSinkConfig sinkConfig, StencilClient stencilClient, StatsDReporter statsDReporter) {
        Descriptors.Descriptor outputMessageDescriptor = stencilClient.get(sinkConfig.getInputSchemaProtoClass());
        Descriptors.Descriptor metadataMessageDescriptor = getMetadataMessageDescriptor(sinkConfig);
        List<WriterPolicy> writerPolicies = new ArrayList<>();
        writerPolicies.add(new TimeBasedRotatingPolicy(sinkConfig.getLocalFileRotationDurationMS()));
        writerPolicies.add(new SizeBasedRotatingPolicy(sinkConfig.getLocalFileRotationMaxSizeBytes()));
        return new LocalStorage(
                sinkConfig,
                outputMessageDescriptor,
                metadataMessageDescriptor.getFields(),
                writerPolicies,
                new FirehoseInstrumentation(statsDReporter, LocalStorage.class));
    }

    /**
     * Creates the remote blob storage backend for the configured provider.
     * <p>
     * Sets the provider-specific {@code *_TYPE} marker in the configuration for the selected storage
     * type (GCS, S3, OSS or COS) and delegates creation to {@link BlobStorageFactory}.
     *
     * @param sinkConfig the blob sink configuration that selects the storage provider
     * @param configuration the configuration to enrich and pass to the storage factory
     * @return the blob storage backend for the configured provider
     * @throws IllegalArgumentException if the configured storage type is not supported
     */
    public static BlobStorage createSinkObjectStorage(BlobSinkConfig sinkConfig, Map<String, String> configuration) {
        switch (sinkConfig.getBlobStorageType()) {
            case GCS:
                configuration.put("GCS_TYPE", "SINK_BLOB");
                break;
            case S3:
                configuration.put("S3_TYPE", "SINK_BLOB");
                break;
            case OSS:
                configuration.put("OSS_TYPE", "SINK_BLOB");
                break;
            case COS:
                configuration.put("COS_TYPE", "SINK_BLOB");
                break;
            default:
                throw new IllegalArgumentException("Sink Blob Storage type " + sinkConfig.getBlobStorageType() + "is not supported");
        }
        return BlobStorageFactory.createObjectStorage(sinkConfig.getBlobStorageType(), configuration);
    }
}
