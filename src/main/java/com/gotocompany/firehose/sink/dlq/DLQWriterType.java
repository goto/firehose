package com.gotocompany.firehose.sink.dlq;

/**
 * The supported dead letter queue writer backends.
 *
 * @see DlqWriterFactory
 */
public enum DLQWriterType {
    /** Write failed messages to a Kafka retry topic. */
    KAFKA,
    /** Write failed messages to blob storage (GCS, S3, OSS or COS). */
    BLOB_STORAGE,
    /** Write failed messages to the application log. */
    LOG
}
