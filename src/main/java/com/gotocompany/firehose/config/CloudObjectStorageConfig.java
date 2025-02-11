package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface CloudObjectStorageConfig extends Config {
    @Key("${COS_TYPE}_COS_REGION")
    String getCosRegion();

    @Key("${COS_TYPE}_COS_BUCKET_NAME")
    String getCosBucketName();

    @Key("${COS_TYPE}_COS_DIRECTORY_PREFIX")
    String getCosDirectoryPrefix();

    @Key("${COS_TYPE}_COS_SECRET_ID")
    String getCosSecretId();

    @Key("${COS_TYPE}_COS_SECRET_KEY")
    String getCosSecretKey();

    @Key("${COS_TYPE}_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS")
    @DefaultValue("1800")
    Integer getCosTempCredentialValiditySeconds();

    @Key("${COS_TYPE}_COS_APPID")
    String getCosAppId();

    @Key("${COS_TYPE}_COS_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getCosRetryMaxAttempts();

    @Key("${COS_TYPE}_COS_RETRY_INITIAL_DELAY_MS")
    @DefaultValue("1000")
    Long getCosRetryInitialDelayMS();

    @Key("${COS_TYPE}_COS_RETRY_MAX_DELAY_MS")
    @DefaultValue("30000")
    Long getCosRetryMaxDelayMS();

    @Key("${COS_TYPE}_COS_RETRY_TOTAL_TIMEOUT_MS")
    @DefaultValue("120000")
    Long getCosRetryTotalTimeoutMS();

    @Key("${COS_TYPE}_COS_CONNECTION_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getCosConnectionTimeoutMS();

    @Key("${COS_TYPE}_COS_SOCKET_TIMEOUT_MS")
    @DefaultValue("50000")
    Long getCosSocketTimeoutMS();
}
