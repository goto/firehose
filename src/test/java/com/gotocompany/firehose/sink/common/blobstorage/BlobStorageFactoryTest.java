package com.gotocompany.firehose.sink.common.blobstorage;

import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.oss.OSS;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class BlobStorageFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldCreateGCSStorage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("GCS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_GCS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_GCS_CREDENTIAL_PATH", "src/test/resources/gcs-credentials.json");
            put("SINK_BLOB_GCS_GOOGLE_CLOUD_PROJECT_ID", "test-project");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
        assertTrue(storage instanceof GoogleCloudStorage);
    }

    @Test
    public void shouldCreateS3Storage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("S3_TYPE", "SINK_BLOB");
            put("SINK_BLOB_S3_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_S3_REGION", "us-east-1");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
        assertTrue(storage instanceof S3);
    }

    @Test
    public void shouldCreateOSSStorage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
        }};

        BlobStorage storage = BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
        assertTrue(storage instanceof OSS);
    }

    @Test
    public void shouldThrowExceptionForInvalidGCSConfig() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Exception while creating GCS Storage");

        Map<String, String> config = new HashMap<String, String>() {{
            put("GCS_TYPE", "SINK_BLOB");
            // Missing required configs
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.GCS, config);
    }

    @Test
    public void shouldThrowExceptionForInvalidS3Config() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Exception while creating S3 Storage");

        Map<String, String> config = new HashMap<String, String>() {{
            put("S3_TYPE", "SINK_BLOB");
            // Missing required configs
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.S3, config);
    }

    @Test
    public void shouldThrowExceptionForInvalidOSSConfig() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Exception while creating OSS Storage");

        Map<String, String> config = new HashMap<String, String>() {{
            put("OSS_TYPE", "SINK_BLOB");
            // Missing required configs
        }};

        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, config);
    }

    @Test
    public void shouldThrowExceptionForUnsupportedStorageType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Blob Storage Type null is not supported");

        BlobStorageFactory.createObjectStorage(null, new HashMap<>());
    }
}
