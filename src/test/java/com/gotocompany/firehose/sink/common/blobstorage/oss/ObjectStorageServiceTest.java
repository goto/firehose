package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.model.*;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageServiceTest {

    @Mock
    private OSS ossClient;

    @Mock
    private OSSConfig ossConfig;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ObjectStorageService objectStorageService;

    @Before
    public void setup() {
        when(ossConfig.getOSSEndpoint()).thenReturn("oss-cn-hangzhou.aliyuncs.com");
        when(ossConfig.getOSSAccessKeyId()).thenReturn("test-access-key");
        when(ossConfig.getOSSAccessKeySecret()).thenReturn("test-secret-key");
        when(ossConfig.getOSSBucketName()).thenReturn("test-bucket");
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn("test/prefix");
        objectStorageService = new ObjectStorageService(ossConfig);
    }

    @Test
    public void shouldStoreContentSuccessfully() throws IOException {
        byte[] content = "test".getBytes();
        ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> contentCaptor = ArgumentCaptor.forClass(byte[].class);

        objectStorageService.store("test-object", content);

        verify(ossClient).putObject(bucketCaptor.capture(), keyCaptor.capture(), contentCaptor.capture());
        assertEquals("test-bucket", bucketCaptor.getValue());
        assertEquals("test/prefix/test-object", keyCaptor.getValue());
        assertEquals(new String(content), new String(contentCaptor.getValue()));
    }

    @Test
    public void shouldHandleNetworkTimeout() {
        ClientException clientException = new ClientException("Connection timeout", "NetworkError", "RequestId123");
        when(ossClient.putObject(anyString(), anyString(), any(byte[].class)))
            .thenThrow(clientException);

        BlobStorageException thrown = assertThrows(
            BlobStorageException.class,
            () -> objectStorageService.store("test-object", "test-content".getBytes())
        );
        assertEquals("NetworkError", thrown.getCode());
        assertEquals("Connection timeout", thrown.getMessage());
    }

    @Test
    public void shouldHandleServerSideError() {
        OSSException ossException = new OSSException(
            "Internal Server Error",
            "InternalError",
            "500",
            "RequestId123",
            "test-bucket"
        );
        when(ossClient.putObject(anyString(), anyString(), any(byte[].class)))
            .thenThrow(ossException);

        BlobStorageException thrown = assertThrows(
            BlobStorageException.class,
            () -> objectStorageService.store("test-object", "test-content".getBytes())
        );
        assertEquals("500", thrown.getCode());
        assertEquals("Internal Server Error", thrown.getMessage());
    }

    @Test
    public void shouldHandleInvalidBucketName() {
        OSSException ossException = new OSSException(
            "The specified bucket is not valid",
            "InvalidBucketName", 
            "400",
            "RequestId123",
            "invalid-bucket"
        );
        when(ossClient.putObject(anyString(), anyString(), any(byte[].class)))
            .thenThrow(ossException);

        BlobStorageException thrown = assertThrows(
            BlobStorageException.class,
            () -> objectStorageService.store("test-object", "test-content".getBytes())
        );
        assertEquals("400", thrown.getCode());
        assertEquals("The specified bucket is not valid", thrown.getMessage());
    }

    @Test
    public void shouldHandleObjectNameWithUnicode() {
        String unicodeObjectName = "测试/文件/测试.txt";
        byte[] content = "test content".getBytes();
        
        objectStorageService.store(unicodeObjectName, content);
        
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(ossClient).putObject(eq("test-bucket"), keyCaptor.capture(), eq(content));
        assertEquals("test/prefix/测试/文件/测试.txt", keyCaptor.getValue());
    }

    @Test
    public void shouldHandleVeryLongObjectNames() {
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longName.append("very-long-name-segment/");
        }
        longName.append("file.txt");
        
        byte[] content = "test content".getBytes();
        objectStorageService.store(longName.toString(), content);
        
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(ossClient).putObject(eq("test-bucket"), keyCaptor.capture(), eq(content));
        assertEquals("test/prefix/" + longName.toString(), keyCaptor.getValue());
    }

    @Test
    public void shouldHandleConcurrentUploads() throws InterruptedException {
        int numThreads = 10;
        CountDownLatch latch = new CountDownLatch(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    objectStorageService.store(
                        "concurrent-test-" + index,
                        ("content-" + index).getBytes()
                    );
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(ossClient, times(numThreads)).putObject(anyString(), anyString(), any(byte[].class));
    }

    @Test
    public void shouldHandleRetryableErrors() {
        OSSException retriableError = new OSSException(
            "Service Unavailable",
            "ServiceUnavailable",
            "503",
            "RequestId123",
            "test-bucket"
        );
        
        when(ossClient.putObject(anyString(), anyString(), any(byte[].class)))
            .thenThrow(retriableError)
            .thenThrow(retriableError)
            .thenReturn(new PutObjectResult());
            
        objectStorageService.store("test-object", "test-content".getBytes());
        
        verify(ossClient, times(3)).putObject(anyString(), anyString(), any(byte[].class));
    }

    @Test
    public void shouldHandleEmptyDirectoryPrefix() {
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn("");
        byte[] content = "test".getBytes();
        
        objectStorageService.store("test-object", content);
        
        verify(ossClient).putObject("test-bucket", "test-object", content);
    }

    @Test
    public void shouldHandleNullDirectoryPrefix() {
        when(ossConfig.getOSSDirectoryPrefix()).thenReturn(null);
        byte[] content = "test".getBytes();
        
        objectStorageService.store("test-object", content);
        
        verify(ossClient).putObject("test-bucket", "test-object", content);
    }

    @Test
    public void shouldHandleMultipleConsecutiveCalls() {
        byte[] content1 = "test1".getBytes();
        byte[] content2 = "test2".getBytes();
        
        objectStorageService.store("object1", content1);
        objectStorageService.store("object2", content2);
        
        verify(ossClient).putObject("test-bucket", "test/prefix/object1", content1);
        verify(ossClient).putObject("test-bucket", "test/prefix/object2", content2);
    }

    @Test
    public void shouldHandleZeroByteContent() {
        byte[] emptyContent = new byte[0];
        objectStorageService.store("empty-file", emptyContent);
        verify(ossClient).putObject("test-bucket", "test/prefix/empty-file", emptyContent);
    }

    @Test
    public void shouldHandleLargeContent() {
        byte[] largeContent = new byte[5 * 1024 * 1024];
        objectStorageService.store("large-file", largeContent);
        verify(ossClient).putObject("test-bucket", "test/prefix/large-file", largeContent);
    }

    @Test
    public void shouldHandleClientShutdown() {
        objectStorageService.close();
        verify(ossClient, times(1)).shutdown();
    }

    @Test
    public void shouldHandleMultipleCloseCalls() {
        objectStorageService.close();
        objectStorageService.close();
        verify(ossClient, times(2)).shutdown();
    }
}
