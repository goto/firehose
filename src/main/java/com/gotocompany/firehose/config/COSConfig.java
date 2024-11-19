package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface COSConfig extends Config {
    @Key("${COS_TYPE}_COS_REGION")
    String getCosRegion();

    @Key("${COS_TYPE}_COS_BUCKET_NAME")
    String getCOSBucketName();

    @Key("${COS_TYPE}_COS_DIRECTORY_PREFIX")
    String getCOSDirectoryPrefix();

    @Key("${COS_TYPE}_COS_SECRET_ID")
    String getCOSSecretId();

    @Key("${COS_TYPE}_COS_SECRET_KEY")
    String getCOSSecretKey();

    @Key("${COS_TYPE}_COS_APPID")
    String getCOSAppId();

    /**
     * @return The valid time of credential.
     * The unit is seconds, the default value is 1800 seconds.
     * Currently, the maximum time for the main account is 2 hours (i.e. 7200 seconds),
     * and the maximum time for the sub-account is 36 hours (i.e. 129600 seconds).
     */
    @Key("${COS_TYPE}_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS")
    @DefaultValue("1800")
    Integer getCOSTempCredentialValiditySeconds();
}
