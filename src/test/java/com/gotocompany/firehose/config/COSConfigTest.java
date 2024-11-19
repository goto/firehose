package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class COSConfigTest {
    @Test
    public void shouldParseConfigForSink() {
        COSConfig cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_DIRECTORY_PREFIX", "prefix");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
            put("SOME_TYPE_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS", "1000");
        }});
        Assert.assertEquals("ap-guangzhou", cosConfig.getCosRegion());
        Assert.assertEquals("bucket", cosConfig.getCOSBucketName());
        Assert.assertEquals("prefix", cosConfig.getCOSDirectoryPrefix());
        Assert.assertEquals("secretId", cosConfig.getCOSSecretId());
        Assert.assertEquals("secretKey", cosConfig.getCOSSecretKey());
        Assert.assertEquals("appid", cosConfig.getCOSAppId());
        Assert.assertEquals((Integer) 1000, cosConfig.getCOSTempCredentialValiditySeconds());
    }

    @Test
    public void shouldParseDefaultCredentialValiditySeconds() {
        COSConfig cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
        }});
        Assert.assertEquals("ap-guangzhou", cosConfig.getCosRegion());
        Assert.assertEquals((Integer) 1800, cosConfig.getCOSTempCredentialValiditySeconds());
    }
}
