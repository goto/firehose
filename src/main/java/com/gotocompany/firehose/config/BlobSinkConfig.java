package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.BlobSinkFilePartitionTypeConverter;
import com.gotocompany.firehose.config.converter.BlobSinkLocalFileWriterTypeConverter;
import com.gotocompany.firehose.config.converter.BlobStorageTypeConverter;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.firehose.sink.blob.Constants;

/**
 * Owner configuration for the blob sink, which batches consumed messages into files and uploads them
 * to object storage.
 *
 * <p>It selects the storage backend, the local staging directory and file-writer type (with Parquet
 * tunables), the rotation policy (by duration and size), the optional Kafka-metadata column, and the
 * time-based file partitioning derived from a protobuf timestamp field. Each accessor maps to an
 * environment variable via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface BlobSinkConfig extends AppConfig {

    /**
     * Returns the object-store backend files are uploaded to, set by {@code SINK_BLOB_STORAGE_TYPE},
     * converted by {@link com.gotocompany.firehose.config.converter.BlobStorageTypeConverter} and
     * defaulting to {@code GCS}.
     *
     * @return the configured {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType}
     */
    @Key("SINK_BLOB_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(BlobStorageTypeConverter.class)
    BlobStorageType getBlobStorageType();

    /**
     * Returns the local directory used to stage files before they are uploaded, set by
     * {@code SINK_BLOB_LOCAL_DIRECTORY} and defaulting to {@code /tmp/firehose}.
     *
     * @return the local staging directory path
     */
    @Key("SINK_BLOB_LOCAL_DIRECTORY")
    @DefaultValue("/tmp/firehose")
    String getLocalDirectory();

    /**
     * Returns the on-disk encoding used for staged files, set by
     * {@code SINK_BLOB_LOCAL_FILE_WRITER_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.BlobSinkLocalFileWriterTypeConverter} and
     * defaulting to {@code parquet}.
     *
     * @return the configured {@code Constants.WriterType}
     */
    @Key("SINK_BLOB_LOCAL_FILE_WRITER_TYPE")
    @DefaultValue("parquet")
    @ConverterClass(BlobSinkLocalFileWriterTypeConverter.class)
    Constants.WriterType getLocalFileWriterType();

    /**
     * Returns the Parquet row-group (block) size in bytes for staged Parquet files, set by
     * {@code SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_BLOCK_SIZE} and defaulting to {@code 134217728}
     * (128 MiB).
     *
     * @return the Parquet block size in bytes
     */
    @Key("SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_BLOCK_SIZE")
    @DefaultValue("134217728")
    int getLocalFileWriterParquetBlockSize();

    /**
     * Returns the Parquet page size in bytes for staged Parquet files, set by
     * {@code SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_PAGE_SIZE} and defaulting to {@code 1048576}
     * (1 MiB).
     *
     * @return the Parquet page size in bytes
     */
    @Key("SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_PAGE_SIZE")
    @DefaultValue("1048576")
    int getLocalFileWriterParquetPageSize();

    /**
     * Returns the column name under which Kafka metadata is nested in the output records, set by
     * {@code SINK_BLOB_OUTPUT_KAFKA_METADATA_COLUMN_NAME} and defaulting to an empty string (top-level
     * metadata).
     *
     * @return the Kafka metadata column name
     */
    @Key("SINK_BLOB_OUTPUT_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getOutputKafkaMetadataColumnName();

    /**
     * Indicates whether Kafka metadata (such as topic, partition and offset) is added to the output
     * records, set by {@code SINK_BLOB_OUTPUT_INCLUDE_KAFKA_METADATA_ENABLE} and defaulting to
     * {@code false}.
     *
     * @return {@code true} if Kafka metadata is included in the output
     */
    @Key("SINK_BLOB_OUTPUT_INCLUDE_KAFKA_METADATA_ENABLE")
    @DefaultValue("false")
    boolean getOutputIncludeKafkaMetadataEnable();

    /**
     * Returns the maximum age in milliseconds of a staged file before it is rotated and uploaded,
     * set by {@code SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS} and defaulting to {@code 3600000}
     * (one hour).
     *
     * @return the file rotation duration in milliseconds
     */
    @Key("SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS")
    @DefaultValue("3600000")
    long getLocalFileRotationDurationMS();

    /**
     * Returns the maximum size in bytes a staged file may reach before it is rotated and uploaded,
     * set by {@code SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES} and defaulting to {@code 268435456}
     * (256 MiB).
     *
     * @return the file rotation size threshold in bytes
     */
    @Key("SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("268435456")
    long getLocalFileRotationMaxSizeBytes();

    /**
     * Returns the protobuf field name holding the timestamp used to derive the time partition of
     * output files, set by {@code SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME}.
     *
     * @return the partition timestamp proto field name
     */
    @Key("SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME")
    String getFilePartitionProtoTimestampFieldName();

    /**
     * Returns the time granularity used to partition output files into directories, set by
     * {@code SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.BlobSinkFilePartitionTypeConverter} and
     * defaulting to {@code day}.
     *
     * @return the configured {@code Constants.FilePartitionType}
     */
    @Key("SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE")
    @DefaultValue("day")
    @ConverterClass(BlobSinkFilePartitionTypeConverter.class)
    Constants.FilePartitionType getFilePartitionTimeGranularityType();

    /**
     * Returns the timezone used to interpret the partition timestamp when building file paths, set
     * by {@code SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE} and defaulting to {@code UTC}.
     *
     * @return the partition timestamp timezone
     */
    @Key("SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE")
    @DefaultValue("UTC")
    String getFilePartitionProtoTimestampTimezone();

    /**
     * Returns the prefix prepended to the date segment of partitioned file paths, set by
     * {@code SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX} and defaulting to {@code dt=}.
     *
     * @return the date partition prefix
     */
    @Key("SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX")
    @DefaultValue("dt=")
    String getFilePartitionTimeDatePrefix();

    /**
     * Returns the prefix prepended to the hour segment of partitioned file paths, set by
     * {@code SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX} and defaulting to {@code hr=}.
     *
     * @return the hour partition prefix
     */
    @Key("SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX")
    @DefaultValue("hr=")
    String getFilePartitionTimeHourPrefix();
}
