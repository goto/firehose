package com.gotocompany.firehose.sink.prometheus;

/**
 * Constants shared across the Prometheus sink.
 * <p>
 * Includes remote-write HTTP header names and defaults, reserved Prometheus label names, the
 * field-mapping validation message, and time-unit scaling factors.
 */
public class PromSinkConstants {

    /** HTTP header name for the content encoding of the remote-write payload. */
    public static final String CONTENT_ENCODING = "Content-Encoding";
    /** HTTP header name carrying the Prometheus remote-write protocol version. */
    public static final String PROMETHEUS_REMOTE_WRITE_VERSION = "X-Prometheus-Remote-Write-Version";
    /** Default content encoding for the remote-write payload (snappy compression). */
    public static final String CONTENT_ENCODING_DEFAULT = "snappy";
    /** Default Prometheus remote-write protocol version sent with each request. */
    public static final String PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT = "0.1.0";

    /** Error message used when no metric field index mapping is configured. */
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    /** Reserved Prometheus label that holds the metric name. */
    public static final String PROMETHEUS_LABEL_FOR_METRIC_NAME = "__name__";
    /** Label name used to record the source Kafka partition on every time series. */
    public static final String KAFKA_PARTITION = "kafka_partition";

    /** Multiplier to convert seconds to milliseconds. */
    public static final long SECONDS_SCALED_TO_MILLI = 1000L;
    /** Divisor to convert nanoseconds to milliseconds. */
    public static final long MILLIS_SCALED_TO_NANOS = 1000000L;
}
