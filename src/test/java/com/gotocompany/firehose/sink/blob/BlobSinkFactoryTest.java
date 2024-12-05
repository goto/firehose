package com.gotocompany.firehose.sink.blob;

import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.blob.writer.WriterOrchestrator;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobSinkFactoryTest {

    @Mock
    private OffsetManager offsetManager;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private BlobSinkConfig sinkConfig;

    private Map<String, String> configuration;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        configuration.put("INPUT_SCHEMA_PROTO_CLASS", "com.test.TestMessage");
        configuration.put("LOCAL_DIRECTORY", "/tmp/test");
        configuration.put("FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE", "UTC");
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorage() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndDirectoryPrefix() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("OSS_DIRECTORY_PREFIX", "test-prefix");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndCustomEndpoint() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "https://custom-oss-endpoint.com");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndRegion() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("OSS_REGION", "oss-test-region");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithGCSStorage() {
        configuration.put("BLOB_STORAGE_TYPE", "GCS");
        configuration.put("GCS_BUCKET_NAME", "test-bucket");
        configuration.put("GCS_GOOGLE_CLOUD_PROJECT_ID", "test-project");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithS3Storage() {
        configuration.put("BLOB_STORAGE_TYPE", "S3");
        configuration.put("S3_BUCKET_NAME", "test-bucket");
        configuration.put("S3_REGION", "us-east-1");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnsupportedStorageType() {
        configuration.put("BLOB_STORAGE_TYPE", "UNKNOWN");
        BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
    }

    @Test
    public void shouldSetOSSTypeInConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.OSS);
        Map<String, String> config = new HashMap<>();
        
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        
        assertTrue(storage instanceof ObjectStorageService);
        assertEquals("SINK_BLOB", config.get("OSS_TYPE"));
    }

    @Test
    public void shouldSetGCSTypeInConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.GCS);
        Map<String, String> config = new HashMap<>();
        
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        
        assertTrue(storage instanceof GoogleCloudStorage);
        assertEquals("SINK_BLOB", config.get("GCS_TYPE"));
    }

    @Test
    public void shouldSetS3TypeInConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.S3);
        Map<String, String> config = new HashMap<>();
        
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, config);
        
        assertTrue(storage instanceof S3);
        assertEquals("SINK_BLOB", config.get("S3_TYPE"));
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndEmptyMetadataColumn() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("OUTPUT_KAFKA_METADATA_COLUMN_NAME", "");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndCustomMetadataColumn() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("OUTPUT_KAFKA_METADATA_COLUMN_NAME", "custom_metadata");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndSizeBasedRotation() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("LOCAL_FILE_ROTATION_MAX_SIZE_BYTES", "1048576");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndTimeBasedRotation() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("LOCAL_FILE_ROTATION_DURATION_MS", "300000");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndBothRotationPolicies() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("LOCAL_FILE_ROTATION_MAX_SIZE_BYTES", "1048576");
        configuration.put("LOCAL_FILE_ROTATION_DURATION_MS", "300000");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndParquetWriter() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("LOCAL_FILE_WRITER_TYPE", "PARQUET");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndCustomParquetConfig() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("LOCAL_FILE_WRITER_TYPE", "PARQUET");
        configuration.put("LOCAL_FILE_WRITER_PARQUET_BLOCK_SIZE", "268435456");
        configuration.put("LOCAL_FILE_WRITER_PARQUET_PAGE_SIZE", "1048576");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndTimePartitioning() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("FILE_PARTITION_TIME_GRANULARITY_TYPE", "HOUR");
        configuration.put("FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME", "event_timestamp");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }

    @Test
    public void shouldCreateBlobSinkWithOSSStorageAndCustomTimePartitionPrefixes() {
        configuration.put("BLOB_STORAGE_TYPE", "OSS");
        configuration.put("OSS_BUCKET_NAME", "test-bucket");
        configuration.put("OSS_ENDPOINT", "oss-test-endpoint");
        configuration.put("OSS_ACCESS_KEY_ID", "test-key");
        configuration.put("OSS_ACCESS_KEY_SECRET", "test-secret");
        configuration.put("FILE_PARTITION_TIME_GRANULARITY_TYPE", "HOUR");
        configuration.put("FILE_PARTITION_TIME_DATE_PREFIX", "date=");
        configuration.put("FILE_PARTITION_TIME_HOUR_PREFIX", "hour=");
        
        Sink sink = BlobSinkFactory.create(configuration, offsetManager, statsDReporter, stencilClient);
        
        assertNotNull(sink);
        assertTrue(sink instanceof BlobSink);
    }
}
