package com.gotocompany.firehose.sink.blob;

import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class BlobSinkFactoryTest {

    @Mock
    private OffsetManager offsetManager;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateBlobSinkWithGCSStorage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("SINK_TYPE", "objectstorage");
            put("SINK_BLOB_STORAGE_TYPE", "GCS");
            put("GCS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_GCS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_GCS_CREDENTIAL_PATH", "src/test/resources/gcs-credentials.json");
            put("SINK_BLOB_GCS_GOOGLE_CLOUD_PROJECT_ID", "test-project");
        }};

        Sink sink = BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithS3Storage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("SINK_TYPE", "objectstorage");
            put("SINK_BLOB_STORAGE_TYPE", "S3");
            put("S3_TYPE", "SINK_BLOB");
            put("SINK_BLOB_S3_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_S3_REGION", "us-east-1");
        }};

        Sink sink = BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorage() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("SINK_TYPE", "objectstorage");
            put("SINK_BLOB_STORAGE_TYPE", "OSS");
            put("OSS_TYPE", "SINK_BLOB");
            put("SINK_BLOB_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
        }};

        Sink sink = BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldThrowExceptionForUnsupportedStorageType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Sink Blob Storage type null is not supported");

        Map<String, String> config = new HashMap<String, String>() {{
            put("SINK_TYPE", "objectstorage");
        }};

        BlobSinkFactory.createSinkObjectStorage(null, config);
    }

    @Test
    public void shouldSetCorrectTypeForGCS() {
        Map<String, String> config = new HashMap<>();
        BlobSinkConfig sinkConfig = mock(BlobSinkConfig.class);
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.GCS);

        BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        assertEquals("SINK_BLOB", config.get("GCS_TYPE"));
    }

    @Test
    public void shouldSetCorrectTypeForS3() {
        Map<String, String> config = new HashMap<>();
        BlobSinkConfig sinkConfig = mock(BlobSinkConfig.class);
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.S3);

        BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        assertEquals("SINK_BLOB", config.get("S3_TYPE"));
    }

    @Test
    public void shouldSetCorrectTypeForOSS() {
        Map<String, String> config = new HashMap<>();
        BlobSinkConfig sinkConfig = mock(BlobSinkConfig.class);
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.OSS);

        BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        assertEquals("SINK_BLOB", config.get("OSS_TYPE"));
    }
}
