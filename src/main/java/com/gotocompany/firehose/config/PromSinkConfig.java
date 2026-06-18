package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;

import java.util.Map;
import java.util.Properties;

/**
 * Owner configuration for the Prometheus (Cortex) sink, which pushes consumed messages as metrics to
 * a Cortex remote-write endpoint.
 *
 * <p>It defines the retryable and loggable HTTP status-code ranges, the request timeout and
 * connection limits, the service URL and headers, the proto-index mappings for metric and label
 * names, and how the event timestamp is sourced. Each accessor maps to an environment variable via
 * {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface PromSinkConfig extends AppConfig {

    /**
     * Returns the HTTP status codes that trigger a retry, set by
     * {@code SINK_PROM_RETRY_STATUS_CODE_RANGES} as inclusive ranges (defaulting to {@code 400-600})
     * and expanded into a membership map by
     * {@link com.gotocompany.firehose.config.converter.RangeToHashMapConverter}.
     *
     * @return a membership map keyed by retryable HTTP status code
     */
    @Key("SINK_PROM_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRetryStatusCodeRanges();

    /**
     * Returns the HTTP status codes whose responses are logged, set by
     * {@code SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES} as inclusive ranges (defaulting to
     * {@code 400-499}) and expanded into a membership map by
     * {@link com.gotocompany.firehose.config.converter.RangeToHashMapConverter}.
     *
     * @return a membership map keyed by loggable HTTP status code
     */
    @Key("SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRequestLogStatusCodeRanges();

    /**
     * Returns the timeout in milliseconds for a remote-write request, set by
     * {@code SINK_PROM_REQUEST_TIMEOUT_MS} and defaulting to {@code 10000}.
     *
     * @return the request timeout in milliseconds
     */
    @Key("SINK_PROM_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkPromRequestTimeoutMs();

    /**
     * Returns the maximum number of concurrent HTTP connections to the Cortex endpoint, set by
     * {@code SINK_PROM_MAX_CONNECTIONS}.
     *
     * @return the maximum connection count
     */
    @Key("SINK_PROM_MAX_CONNECTIONS")
    Integer getSinkPromMaxConnections();

    /**
     * Returns the Cortex remote-write endpoint URL metrics are pushed to, set by
     * {@code SINK_PROM_SERVICE_URL}.
     *
     * @return the Cortex service URL
     */
    @Key("SINK_PROM_SERVICE_URL")
    String getSinkPromServiceUrl();

    /**
     * Returns the additional HTTP headers sent with each remote-write request, set by
     * {@code SINK_PROM_HEADERS} as comma-separated {@code key:value} pairs and defaulting to an empty
     * string.
     *
     * @return the configured request headers
     */
    @Key("SINK_PROM_HEADERS")
    @DefaultValue("")
    String getSinkPromHeaders();

    /**
     * Returns the mapping from protobuf field indices to Prometheus metric names, set by
     * {@code SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING} and parsed from JSON by
     * {@link com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter}.
     *
     * @return the metric-name proto-index mapping as a {@code Properties} tree
     */
    @Key("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromMetricNameProtoIndexMapping();

    /**
     * Returns the mapping from protobuf field indices to Prometheus label names, set by
     * {@code SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING} and parsed from JSON by
     * {@link com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter}.
     *
     * @return the label-name proto-index mapping as a {@code Properties} tree
     */
    @Key("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromLabelNameProtoIndexMapping();

    /**
     * Returns the protobuf field index holding the event timestamp used for each metric sample, set
     * by {@code SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX} and defaulting to {@code 1}.
     *
     * @return the event-timestamp proto field index
     */
    @Key("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX")
    @DefaultValue("1")
    Integer getSinkPromProtoEventTimestampIndex();

    /**
     * Indicates whether the sample time is taken from the message's event timestamp (rather than the
     * ingestion time), set by {@code SINK_PROM_WITH_EVENT_TIMESTAMP} and defaulting to {@code false}.
     *
     * @return {@code true} if the event timestamp is used for metric samples
     */
    @Key("SINK_PROM_WITH_EVENT_TIMESTAMP")
    @DefaultValue("false")
    boolean isEventTimestampEnabled();
}
