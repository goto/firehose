package com.gotocompany.firehose.config.enums;

/**
 * Enumerates the destination sinks that a Firehose deployment can stream Kafka messages into.
 *
 * <p>The active sink is resolved from the {@code SINK_TYPE} setting (via
 * {@link com.gotocompany.firehose.config.converter.SinkTypeConverter}) and is read by the sink
 * factory to instantiate the matching sink implementation. Selecting a value that the factory
 * does not handle results in a configuration error at startup.
 */
public enum SinkType {
    /**
     * Writes messages to a relational database over JDBC (for example PostgreSQL).
     */
    JDBC,
    /**
     * @deprecated
     * HTTP is deprecated from Firehose v0.8.11 onwards. Please consider using HTTPV2 instead.
     */
    @Deprecated
    HTTP,
    /**
     * Writes messages to an HTTP endpoint using the newer depot-backed HTTP sink implementation.
     */
    HTTPV2,
    /**
     * Logs each consumed message, primarily useful for debugging and local development.
     */
    LOG,
    /**
     * Identifies the CleverTap sink. This constant is declared but is not wired into the current
     * sink factory, so selecting it raises a configuration error in this version.
     */
    CLEVERTAP,
    /**
     * Writes messages as time-series points to an InfluxDB database.
     */
    INFLUXDB,
    /**
     * Indexes messages into an Elasticsearch cluster.
     */
    ELASTICSEARCH,
    /**
     * Writes messages to a Redis data store.
     */
    REDIS,
    /**
     * Forwards messages to a gRPC service endpoint.
     */
    GRPC,
    /**
     * Pushes messages as metrics to a Cortex/Prometheus remote-write endpoint.
     */
    PROMETHEUS,
    /**
     * Batches messages into files and uploads them to blob storage (such as Google Cloud Storage,
     * Amazon S3, Alibaba OSS or Tencent COS).
     */
    BLOB,
    /**
     * Streams messages into Google BigQuery tables using the depot BigQuery sink.
     */
    BIGQUERY,
    /**
     * Writes messages into Google Cloud Bigtable using the depot Bigtable sink.
     */
    BIGTABLE,
    /**
     * Writes messages as documents into a MongoDB collection.
     */
    MONGODB,
    /**
     * Streams messages into Alibaba Cloud MaxCompute tables using the depot MaxCompute sink.
     */
    MAXCOMPUTE
}
