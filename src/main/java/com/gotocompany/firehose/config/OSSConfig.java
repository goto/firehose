package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * OSS_TYPE needs to be set as SINK_BLOB or DLQ_BLOB_STORAGE.
 */
public interface OSSConfig extends Config {

    @Key("${OSS_TYPE}_OSS_ENDPOINT")
    String getOSSEndpoint();

    @Key("${OSS_TYPE}_OSS_BUCKET_NAME")
    String getOSSBucketName();

    @Key("${OSS_TYPE}_OSS_DIRECTORY_PREFIX")
    String getOSSDirectoryPrefix();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY_ID")
    String getOSSAccessKeyId();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY_SECRET")
    String getOSSAccessKeySecret();

    /**
     * @return Total retry attempts for OSS object storage.
     */
    @Key("${OSS_TYPE}_OSS_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getOSSRetryMaxAttempts();

    /**
     * @return Total Timeout after which retries will fail.
     */
    @Key("${OSS_TYPE}_OSS_RETRY_TOTAL_TIMEOUT_MS")
    @DefaultValue("120000")
    Long getOSSRetryTotalTimeoutMS();

    /**
     * @return Initial delay before retrying.
     */
    @Key("${OSS_TYPE}_OSS_RETRY_INITIAL_DELAY_MS")
    @DefaultValue("1000")
    Long getOSSRetryInitialDelayMS();

    /**
     * @return Max delay before each retry
     */
    @Key("${OSS_TYPE}_OSS_RETRY_MAX_DELAY_MS")
    @DefaultValue("30000")
    Long getOSSRetryMaxDelayMS();
}
