package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * GCS_TYPE needs to be set as SINK_BLOB or DLQ_BLOB_STORAGE.
 */
public interface GCSConfig extends Config {

    /**
     * Returns the Google Cloud project id that owns the bucket, set by
     * ${GCS_TYPE}_GCS_GOOGLE_CLOUD_PROJECT_ID.
     *
     * @return the Google Cloud project id
     */
    @Key("${GCS_TYPE}_GCS_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    /**
     * Returns the name of the GCS bucket objects are written to, set by
     * ${GCS_TYPE}_GCS_BUCKET_NAME.
     *
     * @return the GCS bucket name
     */
    @Key("${GCS_TYPE}_GCS_BUCKET_NAME")
    String getGCSBucketName();

    /**
     * Returns the object-name (directory) prefix prepended to uploaded objects, set by
     * ${GCS_TYPE}_GCS_DIRECTORY_PREFIX.
     *
     * @return the GCS object directory prefix
     */
    @Key("${GCS_TYPE}_GCS_DIRECTORY_PREFIX")
    String getGCSDirectoryPrefix();

    /**
     * Returns the path to the service-account credentials file used to authenticate to GCS, set by
     * ${GCS_TYPE}_GCS_CREDENTIAL_PATH.
     *
     * @return the GCS credentials file path
     */
    @Key("${GCS_TYPE}_GCS_CREDENTIAL_PATH")
    String getGCSCredentialPath();

    /**
     * @return Total retry attempts for GCS object storage.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getGCSRetryMaxAttempts();

    /**
     * @return Total Timeout after which retries will fail.
     * By default, we can put this large, so that at-least all the retries can happen.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_TOTAL_TIMEOUT_MS")
    @DefaultValue("120000")
    Long getGCSRetryTotalTimeoutMS();

    /**
     * @return Initial delay before retrying.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_INITIAL_DELAY_MS")
    @DefaultValue("1000")
    Long getGCSRetryInitialDelayMS();

    /**
     * @return Max delay before each retry
     */
    @Key("${GCS_TYPE}_GCS_RETRY_MAX_DELAY_MS")
    @DefaultValue("30000")
    Long getGCSRetryMaxDelayMS();

    /**
     * @return The multiplier for the initial delay.
     * For the default value of 2, the delay will be doubled for each retry.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_DELAY_MULTIPLIER")
    @DefaultValue("2")
    Long getGCSRetryDelayMultiplier();

    /**
     * Returns the timeout in milliseconds applied to the first GCS RPC attempt, set by
     * ${GCS_TYPE}_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS and defaulting to {@code 5000}.
     *
     * @return the initial GCS RPC timeout in milliseconds
     */
    @Key("${GCS_TYPE}_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getGCSRetryInitialRPCTimeoutMS();

    /**
     * @return Multiplier of 1 means that the timeout will be constant.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER")
    @DefaultValue("1")
    Long getGCSRetryRPCTimeoutMultiplier();

    /**
     * Returns the maximum timeout in milliseconds for a single GCS RPC attempt, set by
     * ${GCS_TYPE}_GCS_RETRY_RPC_MAX_TIMEOUT_MS and defaulting to {@code 5000}.
     *
     * @return the maximum GCS RPC timeout in milliseconds
     */
    @Key("${GCS_TYPE}_GCS_RETRY_RPC_MAX_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getGCSRetryRPCMaxTimeoutMS();
}


