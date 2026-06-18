package com.gotocompany.firehose.metrics;

/**
 * Metric names, tags, and enums for the BigQuery sink.
 *
 * <p>Defines the BigQuery-specific operation, latency, and error metric names (built on the shared
 * {@link Metrics} prefixes) along with the tag templates and the {@link BigQueryAPIType} and
 * {@link BigQueryErrorType} enums used to label them.
 */
public class BigQueryMetrics {
    /**
     * BigQuery API operations that Firehose instruments.
     */
    public enum BigQueryAPIType {
        /** Updating an existing table. */
        TABLE_UPDATE,
        /** Creating a new table. */
        TABLE_CREATE,
        /** Updating an existing dataset. */
        DATASET_UPDATE,
        /** Creating a new dataset. */
        DATASET_CREATE,
        /** Inserting rows through the insertAll API. */
        TABLE_INSERT_ALL,
    }

    /**
     * Categories of error reported by the BigQuery sink.
     */
    public enum BigQueryErrorType {
        /** An error that does not match a known category. */
        UNKNOWN_ERROR,
        /** The row or table schema is invalid. */
        INVALID_SCHEMA_ERROR,
        /** An out-of-bounds error. */
        OOB_ERROR,
        /** Processing was stopped before completion. */
        STOPPED_ERROR,
    }

    /** Prefix for BigQuery-sink metric names. */
    public static final String BIGQUERY_SINK_PREFIX = "bigquery_";
    /** Tag template for the BigQuery table name. */
    public static final String BIGQUERY_TABLE_TAG = "table=%s";
    /** Tag template for the BigQuery dataset name. */
    public static final String BIGQUERY_DATASET_TAG = "dataset=%s";
    /** Tag template for the BigQuery API operation. */
    public static final String BIGQUERY_API_TAG = "api=%s";
    /** Tag template for the BigQuery error type. */
    public static final String BIGQUERY_ERROR_TAG = "error=%s";
    // BigQuery SINK MEASUREMENTS
    /** Counter of BigQuery API operations, tagged by API type. */
    public static final String SINK_BIGQUERY_OPERATION_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_total";
    /** Timer for BigQuery API operation latency, in milliseconds. */
    public static final String SINK_BIGQUERY_OPERATION_LATENCY_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_latency_milliseconds";
    /** Counter of BigQuery errors, tagged by error type. */
    public static final String SINK_BIGQUERY_ERRORS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "errors_total";

}
