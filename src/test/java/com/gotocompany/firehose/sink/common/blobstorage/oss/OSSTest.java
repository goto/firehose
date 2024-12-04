package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class OSSTest {

    @Mock
    private OSSClient ossClient;

    @Mock
    private OSSConfig ossConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private com.gotocompany.firehose.sink.common.blobstorage.oss.OSS oss;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(ossConfig.getOSSBucketName()).thenReturn("test-bucket");
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn("test-prefix");
        oss = new com.gotocompany.firehose.sink.common.blobstorage.oss.OSS(ossConfig, ossClient);
    }

    @Test
    public void shouldStoreFileWithPrefix() throws BlobStorageException {
        String objectName = "test-object";
        String filePath = "/tmp/test-file";
        
        oss.store(objectName, filePath);
        
        verify(ossClient).putObject(eq("test-bucket"), 
            eq("test-prefix/test-object"), 
            any(File.class));
    }

    @Test
    public void shouldStoreFileWithoutPrefix() throws BlobStorageException {
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn("");
        String objectName = "test-object";
        String filePath = "/tmp/test-file";
        
        oss.store(objectName, filePath);
        
        verify(ossClient).putObject(eq("test-bucket"), 
            eq("test-object"), 
            any(File.class));
    }

    @Test
    public void shouldStoreContentWithPrefix() throws BlobStorageException {
        String objectName = "test-object";
        byte[] content = "test-content".getBytes();
        
        oss.store(objectName, content);
        
        verify(ossClient).putObject(eq("test-bucket"), 
            eq("test-prefix/test-object"), 
            any());
    }

    @Test
    public void shouldStoreContentWithoutPrefix() throws BlobStorageException {
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn("");
        String objectName = "test-object";
        byte[] content = "test-content".getBytes();
        
        oss.store(objectName, content);
        
        verify(ossClient).putObject(eq("test-bucket"), 
            eq("test-object"), 
            any());
    }

    @Test
    public void shouldHandleOSSExceptionWhenStoringFile() throws BlobStorageException {
        expectedException.expect(BlobStorageException.class);
        expectedException.expectMessage("Failed to upload file to OSS");
        
        doThrow(new OSSException("OSS Error", "requestId", "hostId"))
            .when(ossClient).putObject(any(), any(), any(File.class));
        
        oss.store("test-object", "/tmp/test-file");
    }

    @Test
    public void shouldHandleOSSExceptionWhenStoringContent() throws BlobStorageException {
        expectedException.expect(BlobStorageException.class);
        expectedException.expectMessage("Failed to upload content to OSS");
        
        doThrow(new OSSException("OSS Error", "requestId", "hostId"))
            .when(ossClient).putObject(any(), any(), any());
        
        oss.store("test-object", "test-content".getBytes());
    }

    @Test
    public void shouldCloseOSSClientOnClose() {
        oss.close();
        verify(ossClient).shutdown();
    }
}
