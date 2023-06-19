package com.gotocompany.firehose.config.enums;

public enum SinkType {
    JDBC,
    /**
     * @deprecated
     * HTTP is deprecated. Please consider using HTTPV2 instead.
     */
    @Deprecated
    HTTP,
    HTTPV2,
    LOG,
    CLEVERTAP,
    INFLUXDB,
    ELASTICSEARCH,
    REDIS,
    GRPC,
    PROMETHEUS,
    BLOB,
    BIGQUERY,
    BIGTABLE,
    MONGODB
}
