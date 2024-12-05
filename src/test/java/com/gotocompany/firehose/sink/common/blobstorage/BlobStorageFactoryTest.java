package com.gotocompany.firehose.sink.common.blobstorage;

import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class BlobStorageFactoryTest {

    @Test
    public void shouldCreateOSSStorageWithBasicConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithDirectoryPrefix() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "prefix");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForOSSWithMissingBucket() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForOSSWithMissingEndpoint() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForOSSWithMissingAccessKeyId() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForOSSWithMissingSecretKey() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test
    public void shouldCreateOSSStorageWithEmptyDirectoryPrefix() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithSpecialCharactersInConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket-123!@#");
            put("SOME_TYPE_OSS_ENDPOINT", "https://oss-endpoint.com");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access@key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret!key#123");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "prefix/path");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateGCSStorage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("GCS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_GCS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_GCS_GOOGLE_CLOUD_PROJECT_ID", "project-id");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
        assertTrue(storage instanceof GoogleCloudStorage);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForGCSWithMissingConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("GCS_TYPE", "SOME_TYPE");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
    }

    @Test
    public void shouldCreateS3Storage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("S3_TYPE", "SOME_TYPE");
            put("SOME_TYPE_S3_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_S3_REGION", "us-east-1");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
        assertTrue(storage instanceof S3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForS3WithMissingConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("S3_TYPE", "SOME_TYPE");
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnsupportedStorageType() {
        Map<String, String> config = new HashMap<>();
        BlobStorageFactory.createObjectStorage(null, config);
    }

    @Test(expected = IllegalArgumentException.class) 
    public void shouldThrowExceptionForEmptyConfig() {
        Map<String, String> config = new HashMap<>();
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForNullConfig() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, null);
    }

    @Test
    public void shouldCreateOSSStorageWithMultipleTypes() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "TYPE1");
            put("TYPE1_OSS_BUCKET_NAME", "bucket1");
            put("TYPE1_OSS_ENDPOINT", "endpoint1");
            put("TYPE1_OSS_ACCESS_KEY_ID", "key1");
            put("TYPE1_OSS_ACCESS_KEY_SECRET", "secret1");
            put("TYPE2_OSS_BUCKET_NAME", "bucket2");
            put("TYPE2_OSS_ENDPOINT", "endpoint2");
            put("TYPE2_OSS_ACCESS_KEY_ID", "key2");
            put("TYPE2_OSS_ACCESS_KEY_SECRET", "secret2");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithLongPrefixPath() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("SOME_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "very/long/nested/directory/path/structure");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithNumericType() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "123");
            put("123_OSS_BUCKET_NAME", "test-bucket");
            put("123_OSS_ENDPOINT", "oss-endpoint");
            put("123_OSS_ACCESS_KEY_ID", "access-key");
            put("123_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithCaseInsensitiveType() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "some_TYPE");
            put("some_TYPE_OSS_BUCKET_NAME", "test-bucket");
            put("some_TYPE_OSS_ENDPOINT", "oss-endpoint");
            put("some_TYPE_OSS_ACCESS_KEY_ID", "access-key");
            put("some_TYPE_OSS_ACCESS_KEY_SECRET", "secret-key");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithMinimalConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "b");
            put("SOME_TYPE_OSS_ENDPOINT", "e");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "k");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "s");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test
    public void shouldCreateOSSStorageWithMaximalConfig() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "test-bucket-" + "x".repeat(50));
            put("SOME_TYPE_OSS_ENDPOINT", "https://" + "x".repeat(100) + ".com");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "x".repeat(100));
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "x".repeat(100));
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "x".repeat(200));
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }
}
