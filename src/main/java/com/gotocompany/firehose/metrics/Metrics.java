package com.gotocompany.firehose.metrics;

/**
 * Central registry of metric names, metric tags, and metric-related enums used across Firehose.
 *
 * <p>Metric names are assembled from reusable prefixes (application, source/kafka, the various sink
 * types, retry, DLQ, global, pipeline, and error) so every emitted metric shares a consistent
 * {@code firehose_...} naming scheme. Tag constants are {@link String#format(String, Object...)}
 * templates such as {@code "type=%s"}. The {@link MessageScope} and {@link MessageType} enums
 * classify the counters, and {@link #tag(String, String)} formats a single {@code key=value} tag.
 */
public class Metrics {
    //APPLICATION PREFIX
    /** Prefix shared by every Firehose metric name. */
    public static final String APPLICATION_PREFIX = "firehose_";

    //SOURCE PREFIXES
    /** Prefix for source-related metric names. */
    public static final String SOURCE_PREFIX = "source_";
    /** Prefix for Kafka-source metric names. */
    public static final String KAFKA_PREFIX = "kafka_";

    //SINK PREFIXES
    /** Prefix for sink-related metric names. */
    public static final String SINK_PREFIX = "sink_";
    /** Prefix for HTTP-sink metric names. */
    public static final String HTTP_SINK_PREFIX = "http_";
    /** Prefix for gRPC-sink metric names. */
    public static final String GRPC_SINK_PREFIX = "grpc_";
    /** Prefix for blob-storage-sink metric names. */
    public static final String BLOB_SINK_PREFIX = "blob_";

    /** Prefix for MongoDB-sink metric names. */
    public static final String MONGO_SINK_PREFIX = "mongo_";


    //RETRY PREFIX
    /** Prefix for retry metric names. */
    public static final String RETRY_PREFIX = "retry_";

    //DLQ PREFIX
    /** Prefix for dead-letter-queue metric names. */
    public static final String DLQ_PREFIX = "dlq_";

    //GLOBAL PREFIX
    /** Prefix for global, pipeline-wide metric names. */
    public static final String GLOBAL_PREFIX = "global_";

    //PIPELINE PREFIX
    /** Prefix for pipeline-latency metric names. */
    public static final String PIPELINE_PREFIX = "pipeline_";

    //ERROR PREFIX
    /** Prefix for error metric names. */
    public static final String ERROR_PREFIX = "error_";

    // SOURCE MEASUREMENTS
    /** Counter of messages removed by filtering at the source. */
    public static final String SOURCE_KAFKA_MESSAGES_FILTER_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_filter_total";
    /** Counter of Kafka offset commits, tagged by success or failure. */
    public static final String SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_commit_total";
    /** Timer for how long processing a batch of partitions takes, in milliseconds. */
    public static final String SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "partitions_process_milliseconds";
    /** Histogram of the number of records pulled per Kafka poll. */
    public static final String SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "pull_batch_size_total";

    // SINK MEASUREMENTS
    /** Counter of messages handled by the sink. */
    public static final String SINK_MESSAGES_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_total";
    /** Timer for sink response time, in milliseconds. */
    public static final String SINK_RESPONSE_TIME_MILLISECONDS = APPLICATION_PREFIX + SINK_PREFIX + "response_time_milliseconds";
    /** Counter of messages dropped by the sink. */
    public static final String SINK_MESSAGES_DROP_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_drop_total";
    /** Counter of HTTP-sink responses, tagged by response code. */
    public static final String SINK_HTTP_RESPONSE_CODE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + HTTP_SINK_PREFIX + "response_code_total";
    /** Histogram of the number of messages pushed to the sink per batch. */
    public static final String SINK_PUSH_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "push_batch_size_total";
    /** Counter of gRPC-sink errors. */
    public static final String SINK_GRPC_ERROR_TOTAL = APPLICATION_PREFIX + GRPC_SINK_PREFIX + "error_total";

    // MONGO SINK MEASUREMENTS
    /** Counter of documents inserted by the MongoDB sink. */
    public static final String SINK_MONGO_INSERTED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "inserted_total";
    /** Counter of documents matched for update by the MongoDB sink. */
    public static final String SINK_MONGO_UPDATED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "updated_total";
    /** Counter of documents actually modified by the MongoDB sink. */
    public static final String SINK_MONGO_MODIFIED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "modified_total";

    // RETRY MEASUREMENT
    /** Counter of messages processed through retries, tagged by type and error. */
    public static final String RETRY_MESSAGES_TOTAL = APPLICATION_PREFIX + RETRY_PREFIX + "messages_total";
    /** Counter of retry attempts made. */
    public static final String RETRY_ATTEMPTS_TOTAL = APPLICATION_PREFIX + RETRY_PREFIX + "attempts_total";
    /** Gauge of the back-off sleep time between retries, in milliseconds. */
    public static final String RETRY_SLEEP_TIME_MILLISECONDS = APPLICATION_PREFIX + RETRY_PREFIX + "backoff_sleep_milliseconds";

    // DLQ MEASUREMENTS
    /** Counter of DLQ write attempts made. */
    public static final String DLQ_RETRY_ATTEMPTS_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + RETRY_PREFIX + "attempts_total";
    /** Counter of messages handled by the DLQ, tagged by type and error. */
    public static final String DLQ_MESSAGES_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + "messages_total";

    // GLOBAL MEASUREMENTS
    /** Counter of messages by pipeline scope (consumer, sink, DLQ, ignored, filtered). */
    public static final String GLOBAL_MESSAGES_TOTAL = APPLICATION_PREFIX + GLOBAL_PREFIX + "messages_total";

    // PIPELINE DURATION MEASUREMENTS
    /** Timer for end-to-end latency from event time to sink, in milliseconds. */
    public static final String PIPELINE_END_LATENCY_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "end_latency_milliseconds";
    /** Timer for latency from consume time to sink, in milliseconds. */
    public static final String PIPELINE_EXECUTION_LIFETIME_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "execution_lifetime_milliseconds";

    // ERROR MEASUREMENT
    /** Event metric emitted when an error occurs. */
    public static final String ERROR_EVENT = APPLICATION_PREFIX + ERROR_PREFIX + "event";
    /** Counter of messages that resulted in an error, tagged by error type. */
    public static final String ERROR_MESSAGES_TOTAL = APPLICATION_PREFIX + ERROR_PREFIX + "messages_total";

    // CONSUMER TAGS
    /** Tag key carrying the Kafka consumer group id. */
    public static final String CONSUMER_GROUP_ID_TAG = "consumer_group_id";

    // EXECUTION TAGS
    /** Tag marking a successful outcome. */
    public static final String SUCCESS_TAG = "success=true";
    /** Tag marking a failed outcome. */
    public static final String FAILURE_TAG = "success=false";
    /** Tag template for the message type (total, success, or failure). */
    public static final String MESSAGE_TYPE_TAG = "type=%s"; // total, success, failure
    /** Tag template for the message scope. */
    public static final String MESSAGE_SCOPE_TAG = "scope=%s";

    //ERROR TAGS
    /** Tag template for the error type. */
    public static final String ERROR_TYPE_TAG = "error_type=%s";

    //DLQ TAGS
    /** Tag template for the DLQ partition date. */
    public static final String DLQ_DATE_TAG = "date=%s";

    // ERROR TAGS
    /** Tag key carrying the error's class name. */
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    /** Value marking an error as non-fatal. */
    public static final String NON_FATAL_ERROR = "nonfatal";
    /** Value marking an error as fatal. */
    public static final String FATAL_ERROR = "fatal";

    /**
     * Formats a single metric tag as {@code key=value}.
     *
     * @param key   the tag key
     * @param value the tag value
     * @return the formatted {@code key=value} tag
     */
    public static String tag(String key, String value) {
        return String.format("%s=%s", key, value);
    }

    // MESSAGE SCOPE
    /**
     * Stage of the pipeline that a global message-count metric is attributed to.
     */
    public enum MessageScope {
        /** Messages pulled from Kafka by the consumer. */
        CONSUMER,
        /** Messages successfully delivered to the sink. */
        SINK,
        /** Messages written to the dead-letter queue. */
        DLQ,
        /** Messages dropped after all handling was exhausted. */
        IGNORED,
        /** Messages removed by a filter. */
        FILTERED
    }

    // MESSAGE TYPE {
    /**
     * Outcome that a message-count metric represents.
     */
    public enum MessageType {
        /** All messages considered. */
        TOTAL,
        /** Messages handled successfully. */
        SUCCESS,
        /** Messages that failed. */
        FAILURE
    }
}
