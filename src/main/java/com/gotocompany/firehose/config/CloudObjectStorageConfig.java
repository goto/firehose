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

}