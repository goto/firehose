package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.BlobStorageTypeConverter;
import com.gotocompany.firehose.config.converter.DlqPartitionKeyTypeConverter;
import com.gotocompany.firehose.config.converter.DlqWriterTypeConverter;
import com.gotocompany.firehose.config.converter.TimeZoneConverter;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.firehose.sink.dlq.DLQWriterType;
import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;

import java.time.ZoneId;

/**
 * Owner configuration for the dead-letter-queue (DLQ) layer that parks messages Firehose fails to
 * deliver.
 *
 * <p>It selects the DLQ writer (log, Kafka or blob storage), the blob-storage backend and its time
 * partitioning, and the retry policy applied before a message is parked. Each accessor maps to an
 * environment variable via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface DlqConfig extends AppConfig {

    /**
     * Returns the DLQ writer backend, set by {@code DLQ_WRITER_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.DlqWriterTypeConverter} and defaulting to
     * {@code LOG}.
     *
     * @return the configured {@link com.gotocompany.firehose.sink.dlq.DLQWriterType}
     */
    @Key("DLQ_WRITER_TYPE")
    @ConverterClass(DlqWriterTypeConverter.class)
    @DefaultValue("LOG")
    DLQWriterType getDlqWriterType();

    /**
     * Returns the object-store backend used when the DLQ writer is blob storage, set by
     * {@code DLQ_BLOB_STORAGE_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.BlobStorageTypeConverter} and defaulting to
     * {@code GCS}.
     *
     * @return the configured {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType}
     */
    @Key("DLQ_BLOB_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(BlobStorageTypeConverter.class)
    BlobStorageType getBlobStorageType();

    /**
     * Returns the maximum number of retry attempts before a message is sent to the DLQ, set by
     * {@code DLQ_RETRY_MAX_ATTEMPTS} and defaulting to {@code 2147483647} ({@code Integer.MAX_VALUE}).
     *
     * @return the maximum DLQ retry attempts
     */
    @Key("DLQ_RETRY_MAX_ATTEMPTS")
    @DefaultValue("2147483647")
    Integer getDlqRetryMaxAttempts();

    /**
     * Indicates whether the consumer fails once DLQ retry attempts are exhausted, set by
     * {@code DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE} and defaulting to {@code false}.
     *
     * @return {@code true} if exhausting DLQ retries should fail the message
     */
    @Key("DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE")
    @DefaultValue("false")
    boolean getDlqRetryFailAfterMaxAttemptEnable();

    /**
     * Indicates whether the DLQ sink is enabled, set by {@code DLQ_SINK_ENABLE} and defaulting to
     * {@code false}.
     *
     * @return {@code true} if the DLQ sink is enabled
     */
    @Key("DLQ_SINK_ENABLE")
    @DefaultValue("false")
    boolean getDlqSinkEnable();

    /**
     * Returns the timezone used to partition DLQ blob files by time, set by
     * {@code DLQ_BLOB_FILE_PARTITION_TIMEZONE}, converted by
     * {@link com.gotocompany.firehose.config.converter.TimeZoneConverter} and defaulting to
     * {@code UTC}.
     *
     * @return the DLQ blob partition timezone as a {@link java.time.ZoneId}
     */
    @Key("DLQ_BLOB_FILE_PARTITION_TIMEZONE")
    @DefaultValue("UTC")
    @ConverterClass(TimeZoneConverter.class)
    ZoneId getDlqBlobFilePartitionTimezone();

    /**
     * Returns the timestamp used to derive the time partition of DLQ blob files, set by
     * {@code DLQ_BLOB_FILE_PARTITION_KEY}, converted by
     * {@link com.gotocompany.firehose.config.converter.DlqPartitionKeyTypeConverter} and defaulting to
     * {@code CONSUME_TIMESTAMP}.
     *
     * @return the configured {@link com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType}
     */
    @Key("DLQ_BLOB_FILE_PARTITION_KEY")
    @DefaultValue("CONSUME_TIMESTAMP")
    @ConverterClass(DlqPartitionKeyTypeConverter.class)
    DlqPartitionKeyType getDlqBlobFilePartitionKey();

}
