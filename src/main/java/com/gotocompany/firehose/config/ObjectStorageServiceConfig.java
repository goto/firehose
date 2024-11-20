package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface ObjectStorageServiceConfig extends Config {

    @Key("${OSS_TYPE}_OSS_ENDPOINT")
    String getOssEndpoint();

    @Key("${OSS_TYPE}_OSS_REGION")
    String getOssRegion();

    @Key("${OSS_TYPE}_OSS_ACCESS_ID")
    String getOssAccessId();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY")
    String getOssAccessKey();
    
    @Key("${OSS_TYPE}_OSS_BUCKET_NAME")
    String getOssBucketName();

    @Key("${OSS_TYPE}_OSS_DIRECTORY_PREFIX")
    String getOssDirectoryPrefix();
}
