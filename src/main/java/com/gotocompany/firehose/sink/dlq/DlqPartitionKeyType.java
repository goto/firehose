package com.gotocompany.firehose.sink.dlq;

/**
 * Selects which message timestamp determines the date partition of a blob storage DLQ file.
 *
 * @see com.gotocompany.firehose.sink.dlq.blobstorage.DlqDateUtils
 */
public enum DlqPartitionKeyType {
    /** Partition by the time the message was produced to Kafka. */
    PRODUCE_TIMESTAMP,
    /** Partition by the time the message was consumed by Firehose. */
    CONSUME_TIMESTAMP
}
