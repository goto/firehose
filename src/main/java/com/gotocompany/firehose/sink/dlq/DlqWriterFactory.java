package com.gotocompany.firehose.sink.dlq;

import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageFactory;
import com.gotocompany.firehose.sink.dlq.blobstorage.BlobStorageDlqWriter;
import com.gotocompany.firehose.sink.dlq.kafka.KafkaDlqWriter;
import com.gotocompany.firehose.sink.dlq.log.LogDlqWriter;
import com.gotocompany.firehose.utils.KafkaProducerTypesMetadata;
import com.gotocompany.firehose.utils.KafkaUtils;
import com.gotocompany.depot.metrics.StatsDReporter;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

/**
 * Factory that builds the {@link DlqWriter} selected by the dead letter queue configuration.
 * <p>
 * Reads {@link com.gotocompany.firehose.config.DlqConfig} and creates the matching writer: a
 * {@link com.gotocompany.firehose.sink.dlq.kafka.KafkaDlqWriter} backed by a tracing Kafka producer, a
 * {@link com.gotocompany.firehose.sink.dlq.blobstorage.BlobStorageDlqWriter} for the configured blob
 * storage provider (GCS, S3, OSS or COS), or a
 * {@link com.gotocompany.firehose.sink.dlq.log.LogDlqWriter} that logs the messages.
 *
 * @see DlqWriter
 */
public class DlqWriterFactory {

    /**
     * Builds a {@link DlqWriter} for the configured dead letter queue type.
     * <p>
     * For the Kafka type a tracing Kafka producer is created for the configured DLQ topic; for blob
     * storage the provider-specific {@code *_TYPE} marker is set and a blob storage backend is created;
     * for the log type a logging writer is returned.
     *
     * @param configuration the raw configuration key-value pairs
     * @param client the reporter used to publish DLQ metrics
     * @param tracer the OpenTracing tracer used to instrument the Kafka producer
     * @return a dead letter queue writer for the configured type
     * @throws IllegalArgumentException if the DLQ writer type or blob storage type is not supported
     */
    public static DlqWriter create(Map<String, String> configuration, StatsDReporter client, Tracer tracer) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, configuration);

        switch (dlqConfig.getDlqWriterType()) {
            case KAFKA:
                DlqKafkaProducerConfig dlqKafkaProducerConfig = ConfigFactory.create(DlqKafkaProducerConfig.class, configuration);
                KafkaProducer<byte[], byte[]> kafkaProducer = KafkaUtils.getKafkaProducer(KafkaProducerTypesMetadata.DLQ, dlqKafkaProducerConfig, configuration);
                TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

                return new KafkaDlqWriter(tracingProducer, dlqKafkaProducerConfig.getDlqKafkaTopic(), new FirehoseInstrumentation(client, KafkaDlqWriter.class));

            case BLOB_STORAGE:
                switch (dlqConfig.getBlobStorageType()) {
                    case GCS:
                        configuration.put("GCS_TYPE", "DLQ");
                        break;
                    case S3:
                        configuration.put("S3_TYPE", "DLQ");
                        break;
                    case OSS:
                        configuration.put("OSS_TYPE", "DLQ");
                        break;
                    case COS:
                        configuration.put("COS_TYPE", "DLQ");
                        break;
                    default:
                        throw new IllegalArgumentException("DLQ Blob Storage type " + dlqConfig.getBlobStorageType() + "is not supported");
                }

                BlobStorage blobStorage = BlobStorageFactory.createObjectStorage(dlqConfig.getBlobStorageType(), configuration);
                return new BlobStorageDlqWriter(blobStorage, dlqConfig, new FirehoseInstrumentation(client, BlobStorageDlqWriter.class));
            case LOG:
                return new LogDlqWriter(new FirehoseInstrumentation(client, LogDlqWriter.class));

            default:
                throw new IllegalArgumentException("DLQ Writer type " + dlqConfig.getDlqWriterType() + " is not supported");
        }
    }
}
