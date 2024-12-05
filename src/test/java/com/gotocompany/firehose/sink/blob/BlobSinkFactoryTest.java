package com.gotocompany.firehose.sink.blob;

import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BlobSinkFactoryTest {

    @Mock
    private BlobSinkConfig sinkConfig;

    @Test
    public void shouldSetGCSTypeForConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.GCS);
        Map<String, String> config = BlobSinkFactory.getStorageConfiguration(sinkConfig);
        assertEquals("SINK_BLOB", config.get("GCS_TYPE"));
    }

    @Test
    public void shouldSetS3TypeForConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.S3);
        Map<String, String> config = BlobSinkFactory.getStorageConfiguration(sinkConfig);
        assertEquals("SINK_BLOB", config.get("S3_TYPE"));
    }

    @Test
    public void shouldSetOSSTypeForConfiguration() {
        when(sinkConfig.getBlobStorageType()).thenReturn(BlobStorageType.OSS);
        Map<String, String> config = BlobSinkFactory.getStorageConfiguration(sinkConfig);
        assertEquals("SINK_BLOB", config.get("OSS_TYPE"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnsupportedStorageType() {
        when(sinkConfig.getBlobStorageType()).thenReturn(null);
        BlobSinkFactory.getStorageConfiguration(sinkConfig);
    }
}
