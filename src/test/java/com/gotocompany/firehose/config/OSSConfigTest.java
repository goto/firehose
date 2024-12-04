package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class OSSConfigTest {

    @Test
    public void shouldLoadOSSConfigWithPrefix() {
        OSSConfig config = ConfigFactory.create(OSSConfig.class, new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SINK_BLOB_OSS_DIRECTORY_PREFIX", "test-prefix");
            put("SINK_BLOB_OSS_RETRY_MAX_ATTEMPTS", "5");
            put("SINK_BLOB_OSS_RETRY_INITIAL_DELAY_MS", "2000");
            put("SINK_BLOB_OSS_RETRY_MAX_DELAY_MS", "60000");
        }});

        assertEquals("oss-cn-hangzhou.aliyuncs.com", config.getOSSEndpoint());
        assertEquals("test-bucket", config.getOSSBucketName());
        assertEquals("test-key", config.getOSSAccessKeyId());
        assertEquals("test-secret", config.getOSSAccessKeySecret());
        assertEquals("test-prefix", config.getOSSDirectoryPrefix());
        assertEquals(Long.valueOf(5), config.getOSSRetryMaxAttempts());
        assertEquals(Long.valueOf(2000), config.getOSSRetryInitialDelayMS());
        assertEquals(Long.valueOf(60000), config.getOSSRetryMaxDelayMS());
    }

    @Test
    public void shouldLoadDefaultValues() {
        OSSConfig config = ConfigFactory.create(OSSConfig.class, new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
        }});

        assertEquals("", config.getOSSDirectoryPrefix());
        assertEquals(Long.valueOf(10), config.getOSSRetryMaxAttempts());
        assertEquals(Long.valueOf(1000), config.getOSSRetryInitialDelayMS());
        assertEquals(Long.valueOf(30000), config.getOSSRetryMaxDelayMS());
    }
}
