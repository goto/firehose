package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ObjectStorageServiceTest {

    @Mock
    private OSS ossClient;

    private OSSConfig ossConfig;
    private ObjectStorageService objectStorageService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_BLOB_OSS_ENDPOINT", "test-endpoint");
            put("SINK_BLOB_OSS_ACCESS_KEY_ID", "test-key");
            put("SINK_BLOB_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SINK_BLOB_OSS_BUCKET_NAME", "test-bucket");
            put("SINK_BLOB_OSS_DIRECTORY_PREFIX", "test-prefix");
            put("OSS_TYPE", "SINK_BLOB");
        }};
        ossConfig = ConfigFactory.create(OSSConfig.class, properties);
        objectStorageService = new ObjectStorageService(ossConfig);
    }

    @Test
    public void shouldUploadFileSuccessfully() {
        doNothing().when(ossClient).putObject(anyString(), anyString(), any(byte[].class));
        objectStorageService.store("test-object", "test-content".getBytes());
        verify(ossClient, times(1)).putObject(eq("test-bucket"), eq("test-prefix/test-object"), any(byte[].class));
    }

    @Test
    public void shouldUploadLocalFileSuccessfully() {
        String localPath = "test/path/file.txt";
        doNothing().when(ossClient).putObject(anyString(), anyString(), anyString());
        objectStorageService.store("test-object", localPath);
        verify(ossClient, times(1)).putObject(eq("test-bucket"), eq("test-prefix/test-object"), eq(localPath));
    }

    @Test
    public void shouldHandleOSSException() {
        OSSException ossException = new OSSException("Test error", "ErrorCode", "RequestId", "HostId", "RawMessage");
        doThrow(ossException).when(ossClient).putObject(anyString(), anyString(), any(byte[].class));
        
        BlobStorageException thrown = Assert.assertThrows(BlobStorageException.class, 
            () -> objectStorageService.store("test-object", "test-content".getBytes()));
        
        Assert.assertEquals("ErrorCode", thrown.getCode());
        Assert.assertEquals("OSS Upload failed", thrown.getMessage());
    }

    @Test
    public void shouldCloseClientOnShutdown() {
        objectStorageService.close();
        verify(ossClient, times(1)).shutdown();
    }
}
