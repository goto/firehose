package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OSSConfigTest {

    @Test
    public void shouldParseConfigForSink() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_BLOB_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key-id");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-key-secret");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_REGION", "cn-hangzhou");
            put("SINK_BLOB_OSS_DIRECTORY_PREFIX", "test-prefix");
            put("OSS_TYPE", "SINK_BLOB");
        }};

        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossConfig.getOSSEndpoint());
        Assert.assertEquals("test-key-id", ossConfig.getOSSAccessKeyId());
        Assert.assertEquals("test-key-secret", ossConfig.getOSSAccessKeySecret());
        Assert.assertEquals("test-bucket", ossConfig.getOSSBucketName());
        Assert.assertEquals("cn-hangzhou", ossConfig.getOSSRegion());
        Assert.assertEquals("test-prefix", ossConfig.getOSSDirectoryPrefix());
        Assert.assertEquals(Integer.valueOf(1024), ossConfig.getOSSMaxConnections());
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSSocketTimeout());
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSConnectionTimeout());
        Assert.assertEquals(Integer.valueOf(3), ossConfig.getOSSMaxErrorRetry());
    }

    @Test
    public void shouldParseConfigForDLQ() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("DLQ_BLOB_STORAGE_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("DLQ_BLOB_STORAGE_OSS_ACCESS_KEY_ID", "test-key-id");
            put("DLQ_BLOB_STORAGE_OSS_ACCESS_KEY_SECRET", "test-key-secret");
            put("DLQ_BLOB_STORAGE_OSS_BUCKET_NAME", "test-bucket");
            put("DLQ_BLOB_STORAGE_OSS_REGION", "cn-hangzhou");
            put("OSS_TYPE", "DLQ_BLOB_STORAGE");
        }};

        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossConfig.getOSSEndpoint());
        Assert.assertEquals("test-key-id", ossConfig.getOSSAccessKeyId());
        Assert.assertEquals("test-key-secret", ossConfig.getOSSAccessKeySecret());
        Assert.assertEquals("test-bucket", ossConfig.getOSSBucketName());
        Assert.assertEquals("cn-hangzhou", ossConfig.getOSSRegion());
    }
}
