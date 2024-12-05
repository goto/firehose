package com.gotocompany.firehose.sink.blob;

import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BlobSinkFactoryTest {

    @Mock
    private BlobSinkConfig sinkConfig;
    
    @Mock
    private OffsetManager offsetManager;
    
    @Mock
    private StatsDReporter statsDReporter;
    
    @Mock
    private StencilClient stencilClient;

    private Map<String, String> configuration;

    @Before
    public void setup() {
        configuration = new HashMap<>();
    }

    @Test
    public void shouldCreateGCSObjectStorage() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.GCS);
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, configuration);
        assertNotNull(storage);
        assertEquals("SINK_BLOB", configuration.get("GCS_TYPE"));
    }

    @Test
    public void shouldCreateS3ObjectStorage() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.S3);
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, configuration);
        assertNotNull(storage);
        assertEquals("SINK_BLOB", configuration.get("S3_TYPE"));
    }

    @Test
    public void shouldCreateOSSObjectStorage() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.OSS);
        BlobStorage storage = BlobSinkFactory.createSinkObjectStorage(sinkConfig, configuration);
        assertNotNull(storage);
        assertEquals("SINK_BLOB", configuration.get("OSS_TYPE"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnsupportedStorageType() {
        when(sinkConfig.getBlobStorageType()).thenReturn(null);
        BlobSinkFactory.createSinkObjectStorage(sinkConfig, configuration);
    }
}
