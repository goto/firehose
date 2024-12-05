package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class OSSConfigTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldParseConfigForSink() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_OBJECT_STORAGE_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_OBJECT_STORAGE_OSS_ACCESS_KEY_ID", "access-key-id");
            put("SINK_OBJECT_STORAGE_OSS_ACCESS_KEY_SECRET", "access-key-secret");
            put("SINK_OBJECT_STORAGE_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_OBJECT_STORAGE_OSS_DIRECTORY_PREFIX", "test-prefix");
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossConfig.getOSSEndpoint());
        Assert.assertEquals("access-key-id", ossConfig.getOSSAccessKeyId());
        Assert.assertEquals("access-key-secret", ossConfig.getOSSAccessKeySecret());
        Assert.assertEquals("test-bucket", ossConfig.getOSSBucketName());
        Assert.assertEquals("test-prefix", ossConfig.getOSSDirectoryPrefix());
    }

    @Test
    public void shouldParseConfigForDLQ() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("DLQ_OBJECT_STORAGE_OSS_ENDPOINT", "oss-cn-beijing.aliyuncs.com");
            put("DLQ_OBJECT_STORAGE_OSS_ACCESS_KEY_ID", "dlq-key-id");
            put("DLQ_OBJECT_STORAGE_OSS_ACCESS_KEY_SECRET", "dlq-key-secret");
            put("DLQ_OBJECT_STORAGE_OSS_BUCKET_NAME", "dlq-bucket");
            put("DLQ_OBJECT_STORAGE_OSS_DIRECTORY_PREFIX", "dlq-prefix");
            put("OSS_TYPE", "DLQ_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("oss-cn-beijing.aliyuncs.com", ossConfig.getOSSEndpoint());
        Assert.assertEquals("dlq-key-id", ossConfig.getOSSAccessKeyId());
        Assert.assertEquals("dlq-key-secret", ossConfig.getOSSAccessKeySecret());
        Assert.assertEquals("dlq-bucket", ossConfig.getOSSBucketName());
        Assert.assertEquals("dlq-prefix", ossConfig.getOSSDirectoryPrefix());
    }

    @Test
    public void shouldUseDefaultMaxConnections() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(1024), ossConfig.getOSSMaxConnections());
    }

    @Test
    public void shouldOverrideDefaultMaxConnections() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_CONNECTIONS", "2048");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(2048), ossConfig.getOSSMaxConnections());
    }

    @Test
    public void shouldUseDefaultSocketTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSSocketTimeout());
    }

    @Test
    public void shouldOverrideDefaultSocketTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_SOCKET_TIMEOUT", "60000");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(60000), ossConfig.getOSSSocketTimeout());
    }

    @Test
    public void shouldUseDefaultConnectionTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(50000), ossConfig.getOSSConnectionTimeout());
    }

    @Test
    public void shouldOverrideDefaultConnectionTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_CONNECTION_TIMEOUT", "60000");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(60000), ossConfig.getOSSConnectionTimeout());
    }

    @Test
    public void shouldUseDefaultMaxErrorRetry() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(3), ossConfig.getOSSMaxErrorRetry());
    }

    @Test
    public void shouldOverrideDefaultMaxErrorRetry() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_ERROR_RETRY", "5");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(5), ossConfig.getOSSMaxErrorRetry());
    }

    @Test
    public void shouldHandleEmptyDirectoryPrefix() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_DIRECTORY_PREFIX", "");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("", ossConfig.getOSSDirectoryPrefix());
    }

    @Test
    public void shouldHandleNullDirectoryPrefix() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertNull(ossConfig.getOSSDirectoryPrefix());
    }

    @Test
    public void shouldHandleSpecialCharactersInBucketName() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_BUCKET_NAME", "my-special-bucket-123");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals("my-special-bucket-123", ossConfig.getOSSBucketName());
    }

    @Test
    public void shouldHandleZeroMaxConnections() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_CONNECTIONS", "0");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(0), ossConfig.getOSSMaxConnections());
    }

    @Test
    public void shouldHandleZeroSocketTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_SOCKET_TIMEOUT", "0");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(0), ossConfig.getOSSSocketTimeout());
    }

    @Test
    public void shouldHandleZeroConnectionTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_CONNECTION_TIMEOUT", "0");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(0), ossConfig.getOSSConnectionTimeout());
    }

    @Test
    public void shouldHandleZeroMaxErrorRetry() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_ERROR_RETRY", "0");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(0), ossConfig.getOSSMaxErrorRetry());
    }

    @Test
    public void shouldHandleNegativeMaxConnections() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_CONNECTIONS", "-1");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), ossConfig.getOSSMaxConnections());
    }

    @Test
    public void shouldHandleNegativeSocketTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_SOCKET_TIMEOUT", "-1");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), ossConfig.getOSSSocketTimeout());
    }

    @Test
    public void shouldHandleNegativeConnectionTimeout() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_CONNECTION_TIMEOUT", "-1");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), ossConfig.getOSSConnectionTimeout());
    }

    @Test
    public void shouldHandleNegativeMaxErrorRetry() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_OBJECT_STORAGE");
            put("SINK_OBJECT_STORAGE_OSS_MAX_ERROR_RETRY", "-1");
        }};
        OSSConfig ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), ossConfig.getOSSMaxErrorRetry());
    }
}
