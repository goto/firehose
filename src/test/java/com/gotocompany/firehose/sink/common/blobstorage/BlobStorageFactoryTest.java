package com.gotocompany.firehose.sink.common.blobstorage;

import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class BlobStorageFactoryTest {

    private Map<String, String> config;

    @Before
    public void setup() {
        config = new HashMap<>();
    }

    @Test
    public void shouldCreateGCSStorage() {
        config.put("GCS_TYPE", "SINK_BLOB");
        config.put("SINK_BLOB_GCS_BUCKET_NAME", "test-bucket");
        config.put("SINK_BLOB_GCS_GOOGLE_CLOUD_PROJECT_ID", "test-project");
        
        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
        assertTrue(storage instanceof GoogleCloudStorage);
    }

    @Test
    public void shouldCreateS3Storage() {
        config.put("S3_TYPE", "SINK_BLOB");
        config.put("SINK_BLOB_S3_BUCKET_NAME", "test-bucket");
        config.put("SINK_BLOB_S3_REGION", "us-east-1");
        
        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
        assertTrue(storage instanceof S3);
    }

    @Test
    public void shouldCreateOSSStorage() {
        config.put("OSS_TYPE", "SINK_BLOB");
        config.put("SINK_BLOB_OSS_ENDPOINT", "test-endpoint");
        config.put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
        config.put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
        config.put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
        
        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof ObjectStorageService);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForGCSWithInvalidConfig() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForS3WithInvalidConfig() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForOSSWithInvalidConfig() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnsupportedType() {
        BlobStorageFactory.createObjectStorage(null, config);
    }
}
