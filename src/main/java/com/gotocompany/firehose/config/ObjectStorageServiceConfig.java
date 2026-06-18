package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration for Alibaba Cloud Object Storage Service (OSS), used by the blob sink and the
 * blob-based DLQ writer.
 *
 * <p>Every key is prefixed with an {@code OSS_TYPE} placeholder that owner expands at runtime to the
 * usage context (for example {@code SINK_BLOB} or {@code DLQ_BLOB_STORAGE}), so a single interface
 * can configure either destination. It supplies the endpoint, region, credentials, bucket and
 * object prefix together with the connection, request and retry policy. Each accessor maps to an
 * environment variable via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface ObjectStorageServiceConfig extends Config {

    /**
     * Returns the OSS service endpoint to connect to, set by ${OSS_TYPE}_OSS_ENDPOINT.
     *
     * @return the OSS endpoint
     */
    @Key("${OSS_TYPE}_OSS_ENDPOINT")
    String getOssEndpoint();

    /**
     * Returns the OSS region of the bucket, set by ${OSS_TYPE}_OSS_REGION.
     *
     * @return the OSS region
     */
    @Key("${OSS_TYPE}_OSS_REGION")
    String getOssRegion();

    /**
     * Returns the OSS access key id used to authenticate, set by ${OSS_TYPE}_OSS_ACCESS_ID.
     *
     * @return the OSS access key id
     */
    @Key("${OSS_TYPE}_OSS_ACCESS_ID")
    String getOssAccessId();

    /**
     * Returns the OSS access key secret used to authenticate, set by ${OSS_TYPE}_OSS_ACCESS_KEY.
     *
     * @return the OSS access key secret
     */
    @Key("${OSS_TYPE}_OSS_ACCESS_KEY")
    String getOssAccessKey();

    /**
     * Returns the name of the OSS bucket objects are written to, set by ${OSS_TYPE}_OSS_BUCKET_NAME.
     *
     * @return the OSS bucket name
     */
    @Key("${OSS_TYPE}_OSS_BUCKET_NAME")
    String getOssBucketName();

    /**
     * Returns the object-name (directory) prefix prepended to uploaded objects, set by
     * ${OSS_TYPE}_OSS_DIRECTORY_PREFIX.
     *
     * @return the OSS object directory prefix
     */
    @Key("${OSS_TYPE}_OSS_DIRECTORY_PREFIX")
    String getOssDirectoryPrefix();

    /**
     * Returns the socket timeout in milliseconds for OSS data transfer, set by
     * ${OSS_TYPE}_OSS_SOCKET_TIMEOUT_MS and defaulting to {@code 50000}.
     *
     * @return the OSS socket timeout in milliseconds
     */
    @Key("${OSS_TYPE}_OSS_SOCKET_TIMEOUT_MS")
    @DefaultValue("50000")
    Integer getOssSocketTimeoutMs();

    /**
     * Returns the timeout in milliseconds for establishing an OSS connection, set by
     * ${OSS_TYPE}_OSS_CONNECTION_TIMEOUT_MS and defaulting to {@code 50000}.
     *
     * @return the OSS connection timeout in milliseconds
     */
    @Key("${OSS_TYPE}_OSS_CONNECTION_TIMEOUT_MS")
    @DefaultValue("50000")
    Integer getOssConnectionTimeoutMs();

    /**
     * Returns the timeout in milliseconds for obtaining a connection from the OSS connection pool,
     * set by ${OSS_TYPE}_OSS_CONNECTION_REQUEST_TIMEOUT_MS and defaulting to {@code -1} (no limit).
     *
     * @return the OSS connection-request timeout in milliseconds, or {@code -1} when unlimited
     */
    @Key("${OSS_TYPE}_OSS_CONNECTION_REQUEST_TIMEOUT_MS")
    @DefaultValue("-1")
    Integer getOssConnectionRequestTimeoutMs();

    /**
     * Returns the overall request timeout in milliseconds for an OSS operation, set by
     * ${OSS_TYPE}_OSS_REQUEST_TIMEOUT_MS and defaulting to {@code 300000}.
     *
     * @return the OSS request timeout in milliseconds
     */
    @Key("${OSS_TYPE}_OSS_REQUEST_TIMEOUT_MS")
    @DefaultValue("300000")
    Integer getOssRequestTimeoutMs();

    /**
     * Indicates whether failed OSS operations are retried, set by ${OSS_TYPE}_OSS_RETRY_ENABLED and
     * defaulting to {@code true}.
     *
     * @return {@code true} if OSS retries are enabled
     */
    @Key("${OSS_TYPE}_OSS_RETRY_ENABLED")
    @DefaultValue("true")
    boolean isRetryEnabled();

    /**
     * Returns the maximum number of retry attempts for an OSS operation, set by
     * ${OSS_TYPE}_OSS_MAX_RETRY_ATTEMPTS and defaulting to {@code 3}.
     *
     * @return the maximum OSS retry attempts
     */
    @Key("${OSS_TYPE}_OSS_MAX_RETRY_ATTEMPTS")
    @DefaultValue("3")
    int getOssMaxRetryAttempts();

}
