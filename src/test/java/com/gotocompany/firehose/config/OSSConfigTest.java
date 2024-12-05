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
            put("SINK_BLOB_OSS_ENDPOINT", "test-endpoint");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_DIRECTORY_PREFIX", "test-prefix");
            put("SINK_BLOB_OSS_MAX_CONNECTIONS", "2048");
            put("SINK_BLOB_OSS_SOCKET_TIMEOUT", "60000");
            put("SINK_BLOB_OSS_CONNECTION_TIMEOUT", "60000");
            put("SINK_BLOB_OSS_MAX_ERROR_RETRY", "5");
            put("OSS_TYPE", "SINK_BLOB");
        }};

        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("test-endpoint", ossConfig.getOSSEndpoint());
        Assert.assertEquals("test-key", ossConfig.getOSSAccessKeyId());
        Assert.assertEquals("test-secret", ossConfig.getOSSAccessKeySecret());
        Assert.assertEquals("test-bucket", ossConfig.getOSSBucketName());
        Assert.assertEquals("test-prefix", ossConfig.getOSSDirectoryPrefix());
        Assert.assertEquals(Integer.valueOf(2048), ossConfig.getOSSMaxConnections());
        Assert.assertEquals(Integer.valueOf(60000), ossConfig.getOSSSocketTimeout());
        Assert.assertEquals(Integer.valueOf(60000), ossConfig.getOSSConnectionTimeout());
        Assert.assertEquals(Integer.valueOf(5), ossConfig.getOSSMaxErrorRetry());
    }

    @Test
    public void shouldUseDefaultValues() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_BLOB_OSS_ENDPOINT", "test-endpoint");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("OSS_TYPE", "SINK_BLOB");
        }};

        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(1024), ossConfig.getOSSMaxConnections());
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSSocketTimeout());
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSConnectionTimeout());
        Assert.assertEquals(Integer.valueOf(3), ossConfig.getOSSMaxErrorRetry());
    }
}
