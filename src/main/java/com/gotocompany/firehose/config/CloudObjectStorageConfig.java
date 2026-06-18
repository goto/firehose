package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration for Tencent Cloud Object Storage (COS), used by the blob sink and the
 * blob-based DLQ writer.
 *
 * <p>Every key is prefixed with a {@code COS_TYPE} placeholder that owner expands at runtime to the
 * usage context (for example {@code SINK_BLOB} or {@code DLQ_BLOB_STORAGE}), so a single interface
 * can configure either destination. It supplies the region, bucket, object prefix and credentials
 * (including temporary-credential validity and the app id) together with the client retry,
 * connection and socket timeout policy. Each accessor maps to an environment variable via
 * {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface CloudObjectStorageConfig extends Config {
    /**
     * Returns the COS region of the bucket, set by ${COS_TYPE}_COS_REGION.
     *
     * @return the COS region
     */
    @Key("${COS_TYPE}_COS_REGION")
    String getCosRegion();

    /**
     * Returns the name of the COS bucket objects are written to, set by ${COS_TYPE}_COS_BUCKET_NAME.
     *
     * @return the COS bucket name
     */
    @Key("${COS_TYPE}_COS_BUCKET_NAME")
    String getCosBucketName();

    /**
     * Returns the object-name (directory) prefix prepended to uploaded objects, set by
     * ${COS_TYPE}_COS_DIRECTORY_PREFIX.
     *
     * @return the COS object directory prefix
     */
    @Key("${COS_TYPE}_COS_DIRECTORY_PREFIX")
    String getCosDirectoryPrefix();

    /**
     * Returns the COS secret id used to authenticate, set by ${COS_TYPE}_COS_SECRET_ID.
     *
     * @return the COS secret id
     */
    @Key("${COS_TYPE}_COS_SECRET_ID")
    String getCosSecretId();

    /**
     * Returns the COS secret key used to authenticate, set by ${COS_TYPE}_COS_SECRET_KEY.
     *
     * @return the COS secret key
     */
    @Key("${COS_TYPE}_COS_SECRET_KEY")
    String getCosSecretKey();

    /**
     * Returns how long in seconds temporary COS credentials remain valid, set by
     * ${COS_TYPE}_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS and defaulting to {@code 1800} (30 minutes).
     *
     * @return the temporary-credential validity in seconds
     */
    @Key("${COS_TYPE}_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS")
    @DefaultValue("1800")
    Integer getCosTempCredentialValiditySeconds();

    /**
     * Returns the Tencent Cloud application id that owns the COS bucket, set by
     * ${COS_TYPE}_COS_APPID.
     *
     * @return the COS application id
     */
    @Key("${COS_TYPE}_COS_APPID")
    String getCosAppId();

    /**
     * Returns the maximum number of retry attempts for a COS operation, set by
     * ${COS_TYPE}_COS_RETRY_MAX_ATTEMPTS and defaulting to {@code 10}.
     *
     * @return the maximum COS retry attempts
     */
    @Key("${COS_TYPE}_COS_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getCosRetryMaxAttempts();

    /**
     * Returns the initial delay in milliseconds before the first COS retry, set by
     * ${COS_TYPE}_COS_RETRY_INITIAL_DELAY_MS and defaulting to {@code 1000}.
     *
     * @return the initial COS retry delay in milliseconds
     */
    @Key("${COS_TYPE}_COS_RETRY_INITIAL_DELAY_MS")
    @DefaultValue("1000")
    Long getCosRetryInitialDelayMS();

    /**
     * Returns the maximum delay in milliseconds between COS retries, set by
     * ${COS_TYPE}_COS_RETRY_MAX_DELAY_MS and defaulting to {@code 30000}.
     *
     * @return the maximum COS retry delay in milliseconds
     */
    @Key("${COS_TYPE}_COS_RETRY_MAX_DELAY_MS")
    @DefaultValue("30000")
    Long getCosRetryMaxDelayMS();

    /**
     * Returns the total timeout in milliseconds after which COS retries stop, set by
     * ${COS_TYPE}_COS_RETRY_TOTAL_TIMEOUT_MS and defaulting to {@code 120000}.
     *
     * @return the total COS retry timeout in milliseconds
     */
    @Key("${COS_TYPE}_COS_RETRY_TOTAL_TIMEOUT_MS")
    @DefaultValue("120000")
    Long getCosRetryTotalTimeoutMS();

    /**
     * Returns the timeout in milliseconds for establishing a COS connection, set by
     * ${COS_TYPE}_COS_CONNECTION_TIMEOUT_MS and defaulting to {@code 5000}.
     *
     * @return the COS connection timeout in milliseconds
     */
    @Key("${COS_TYPE}_COS_CONNECTION_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getCosConnectionTimeoutMS();

    /**
     * Returns the socket timeout in milliseconds for COS data transfer, set by
     * ${COS_TYPE}_COS_SOCKET_TIMEOUT_MS and defaulting to {@code 50000}.
     *
     * @return the COS socket timeout in milliseconds
     */
    @Key("${COS_TYPE}_COS_SOCKET_TIMEOUT_MS")
    @DefaultValue("50000")
    Long getCosSocketTimeoutMS();

    /**
     * Returns the delay in milliseconds applied between COS retries, set by
     * ${COS_TYPE}_COS_RETRY_DELAY_MS and defaulting to {@code 1000}.
     *
     * @return the COS retry delay in milliseconds
     */
    @Key("${COS_TYPE}_COS_RETRY_DELAY_MS")
    @DefaultValue("1000")
    Long getCosRetryDelayMS();
}
