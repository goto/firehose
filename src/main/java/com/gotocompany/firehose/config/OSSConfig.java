package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface OSSConfig extends Config {

    @Key("${OSS_TYPE}_OSS_ENDPOINT")
    String getOSSEndpoint();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY_ID")
    String getOSSAccessKeyId();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY_SECRET")
    String getOSSAccessKeySecret();

    @Key("${OSS_TYPE}_OSS_BUCKET_NAME")
    String getOSSBucketName();

    @Key("${OSS_TYPE}_OSS_DIRECTORY_PREFIX")
    String getOSSDirectoryPrefix();

    @Key("${OSS_TYPE}_OSS_MAX_CONNECTIONS")
    @DefaultValue("1024")
    Integer getOSSMaxConnections();

    @Key("${OSS_TYPE}_OSS_SOCKET_TIMEOUT")
    @DefaultValue("50000")
    Integer getOSSSocketTimeout();

    @Key("${OSS_TYPE}_OSS_CONNECTION_TIMEOUT")
    @DefaultValue("50000")
    Integer getOSSConnectionTimeout();

    @Key("${OSS_TYPE}_OSS_MAX_ERROR_RETRY")
    @DefaultValue("3")
    Integer getOSSMaxErrorRetry();
}
