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

public class DlqWriterFactory {

    public static DlqWriter create(Map<String, String> configuration, StatsDReporter client, Tracer tracer) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, configuration);
        FirehoseInstrumentation instrumentation = new FirehoseInstrumentation(client, DlqWriterFactory.class);
        
        instrumentation.logInfo("Creating DLQ writer of type: {}", dlqConfig.getDlqWriterType());

        switch (dlqConfig.getDlqWriterType()) {
            case KAFKA:
                DlqKafkaProducerConfig dlqKafkaProducerConfig = ConfigFactory.create(DlqKafkaProducerConfig.class, configuration);
                KafkaProducer<byte[], byte[]> kafkaProducer = KafkaUtils.getKafkaProducer(KafkaProducerTypesMetadata.DLQ, dlqKafkaProducerConfig, configuration);
                TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

                return new KafkaDlqWriter(tracingProducer, dlqKafkaProducerConfig.getDlqKafkaTopic(), new FirehoseInstrumentation(client, KafkaDlqWriter.class));

            case BLOB_STORAGE:
                instrumentation.logInfo("DLQ blob storage type: {}", dlqConfig.getBlobStorageType());
                
                switch (dlqConfig.getBlobStorageType()) {
                    case GCS:
                        configuration.put("GCS_TYPE", "DLQ");
                        break;
                    case S3:
                        configuration.put("S3_TYPE", "DLQ");
                        break;
                    case OSS:
                        configuration.put("OSS_TYPE", "DLQ");
                        instrumentation.logInfo("OSS DLQ Configuration - endpoint: {}, region: {}, bucket: {}, directoryPrefix: {}", 
                            configuration.get("DLQ_OSS_ENDPOINT"),
                            configuration.get("DLQ_OSS_REGION"),
                            configuration.get("DLQ_OSS_BUCKET_NAME"),
                            configuration.get("DLQ_OSS_DIRECTORY_PREFIX"));
                        instrumentation.logInfo("OSS DLQ Retry Configuration - enabled: {}, maxAttempts: {}", 
                            configuration.getOrDefault("DLQ_OSS_RETRY_ENABLED", "true"),
                            configuration.getOrDefault("DLQ_OSS_MAX_RETRY_ATTEMPTS", "3"));
                        instrumentation.logDebug("OSS DLQ Timeout Configuration - socketTimeout: {}ms, connectionTimeout: {}ms, requestTimeout: {}ms", 
                            configuration.getOrDefault("DLQ_OSS_SOCKET_TIMEOUT_MS", "50000"),
                            configuration.getOrDefault("DLQ_OSS_CONNECTION_TIMEOUT_MS", "50000"),
                            configuration.getOrDefault("DLQ_OSS_REQUEST_TIMEOUT_MS", "300000"));
                        break;
                    case COS:
                        configuration.put("COS_TYPE", "DLQ");
                        break;
                    default:
                        throw new IllegalArgumentException("DLQ Blob Storage type " + dlqConfig.getBlobStorageType() + "is not supported");
                }
                
                instrumentation.logInfo("DLQ blob file partition timezone: {}", dlqConfig.getDlqBlobFilePartitionTimezone());
                BlobStorage blobStorage = BlobStorageFactory.createObjectStorage(dlqConfig.getBlobStorageType(), configuration);
                return new BlobStorageDlqWriter(blobStorage, dlqConfig, new FirehoseInstrumentation(client, BlobStorageDlqWriter.class));
            case LOG:
                return new LogDlqWriter(new FirehoseInstrumentation(client, LogDlqWriter.class));

            default:
                throw new IllegalArgumentException("DLQ Writer type " + dlqConfig.getDlqWriterType() + " is not supported");
        }
    }
}
