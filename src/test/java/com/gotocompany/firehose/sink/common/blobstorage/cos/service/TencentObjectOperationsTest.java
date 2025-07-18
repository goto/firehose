package com.gotocompany.firehose.sink.common.blobstorage.cos.service;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.error.COSErrorType;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.ObjectMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TencentObjectOperationsTest {

    @Mock
    private COSClient cosClient;

    @Mock
    private CloudObjectStorageConfig config;

    private TencentObjectOperations operations;
    private Path tempDir;

    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "test/object.txt";
    private static final int MAX_RETRIES = 3;
    private static final long CONNECTION_TIMEOUT = 5000L;
    private static final long SOCKET_TIMEOUT = 5000L;

    @Before
    public void setUp() throws IOException {
        when(config.getCosBucketName()).thenReturn(BUCKET_NAME);
        when(config.getCosRetryMaxAttempts()).thenReturn(MAX_RETRIES);
        when(config.getCosConnectionTimeoutMS()).thenReturn(CONNECTION_TIMEOUT);
        when(config.getCosSocketTimeoutMS()).thenReturn(SOCKET_TIMEOUT);

        when(cosClient.putObject(anyString(), anyString(), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
            .thenReturn(null);
        when(cosClient.putObject(anyString(), anyString(), any(File.class)))
            .thenReturn(null);

        operations = new TencentObjectOperations(cosClient, config);
        tempDir = Files.createTempDirectory("cos-test");
    }

    @Test
    public void shouldUploadByteArraySuccessfully() throws BlobStorageException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        operations.uploadObject(OBJECT_KEY, content);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == content.length)
        );
    }

    @Test
    public void shouldUploadFileSuccessfully() throws BlobStorageException, IOException {
        Path tempFile = tempDir.resolve("test.txt");
        Files.write(tempFile, "test content".getBytes(StandardCharsets.UTF_8));
        operations.uploadObject(OBJECT_KEY, tempFile.toString());
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            eq(tempFile.toFile())
        );
    }

    @Test
    public void shouldHandleNullObjectKey() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> operations.uploadObject(null, content));
    }

    @Test
    public void shouldHandleEmptyObjectKey() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> operations.uploadObject("", content));
    }

    @Test
    public void shouldHandleNullContent() {
        assertThrows(IllegalArgumentException.class, () -> operations.uploadObject(OBJECT_KEY, (byte[]) null));
    }

    @Test
    public void shouldHandleEmptyContent() throws BlobStorageException {
        byte[] content = new byte[0];
        operations.uploadObject(OBJECT_KEY, content);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == 0)
        );
    }

    @Test
    public void shouldHandleLargeContent() throws BlobStorageException {
        byte[] content = new byte[10 * 1024 * 1024];
        operations.uploadObject(OBJECT_KEY, content);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == content.length)
        );
    }

    @Test
    public void shouldHandleSpecialCharactersInObjectKey() throws BlobStorageException {
        String objectKey = "test/特殊字符/!@#$%^&*()/🔑🗝️.txt";
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        operations.uploadObject(objectKey, content);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(objectKey),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == content.length)
        );
    }

    @Test
    public void shouldHandleVeryLongObjectKey() throws BlobStorageException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("very/long/path/");
        }
        sb.append("file.txt");
        String objectKey = sb.toString();
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        operations.uploadObject(objectKey, content);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(objectKey),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == content.length)
        );
    }

    @Test
    public void shouldHandleNonExistentFile() {
        String nonExistentFile = tempDir.resolve("nonexistent.txt").toString();
        assertThrows(BlobStorageException.class, () -> operations.uploadObject(OBJECT_KEY, nonExistentFile));
    }

    @Test
    public void shouldHandleDirectoryAsFile() throws IOException {
        Path directory = tempDir.resolve("testdir");
        Files.createDirectory(directory);
        assertThrows(BlobStorageException.class, () -> operations.uploadObject(OBJECT_KEY, directory.toString()));
    }

    @Test
    public void shouldHandleEmptyFile() throws BlobStorageException, IOException {
        Path emptyFile = tempDir.resolve("empty.txt");
        Files.createFile(emptyFile);
        operations.uploadObject(OBJECT_KEY, emptyFile.toString());
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            eq(emptyFile.toFile())
        );
    }

    @Test
    public void shouldHandleLargeFile() throws BlobStorageException, IOException {
        Path largeFile = tempDir.resolve("large.txt");
        byte[] content = new byte[10 * 1024 * 1024];
        Files.write(largeFile, content);
        operations.uploadObject(OBJECT_KEY, largeFile.toString());
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            eq(largeFile.toFile())
        );
    }

    @Test
    public void shouldHandleServiceErrorWithRetry() throws BlobStorageException {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(500);

        when(cosClient.putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        ))
            .thenThrow(serviceException)
            .thenThrow(serviceException)
            .thenReturn(null);

        operations.uploadObject(OBJECT_KEY, content);
        verify(cosClient, times(3)).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldHandleClientErrorWithRetry() throws BlobStorageException {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        when(cosClient.putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        ))
            .thenThrow(new CosClientException("Network error"))
            .thenThrow(new CosClientException("Network error"))
            .thenReturn(null);

        operations.uploadObject(OBJECT_KEY, content);
        verify(cosClient, times(3)).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldHandleInterruptedThread() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(503);
        when(cosClient.putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        ))
            .thenThrow(serviceException);

        Thread.currentThread().interrupt();
        try {
            operations.uploadObject(OBJECT_KEY, content);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertTrue(Thread.interrupted());
            assertEquals(COSErrorType.DEFAULT_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleAllHttpStatusCodes() throws BlobStorageException {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(429);
        when(cosClient.putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
            .thenThrow(serviceException);

        try {
            operations.uploadObject(OBJECT_KEY, content);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.TOO_MANY_REQUESTS.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleNullCosClient() {
        assertThrows(IllegalArgumentException.class, () -> new TencentObjectOperations(null, config));
    }

    @Test
    public void shouldHandleNullConfig() {
        assertThrows(IllegalArgumentException.class, () -> new TencentObjectOperations(cosClient, null));
    }

    @Test
    public void shouldHandleDeleteObjectSuccessfully() throws BlobStorageException {
        operations.deleteObject(OBJECT_KEY);
        verify(cosClient).deleteObject(BUCKET_NAME, OBJECT_KEY);
    }

    @Test
    public void shouldHandleDeleteNonExistentObject() throws BlobStorageException {
        CosServiceException serviceException = new CosServiceException("Not found");
        serviceException.setStatusCode(404);
        doThrow(serviceException).when(cosClient).deleteObject(eq(BUCKET_NAME), eq(OBJECT_KEY));

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.NOT_FOUND.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithServiceError() throws BlobStorageException {
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(500);
        doThrow(serviceException).when(cosClient).deleteObject(eq(BUCKET_NAME), eq(OBJECT_KEY));

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.INTERNAL_SERVER_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithClientError() throws BlobStorageException {
        CosClientException clientException = new CosClientException("Client error");
        doThrow(clientException).when(cosClient).deleteObject(eq(BUCKET_NAME), eq(OBJECT_KEY));

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.INTERNAL_SERVER_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithInterruptedThread() {
        Thread.currentThread().interrupt();
        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertTrue(Thread.interrupted());
            assertEquals(COSErrorType.DEFAULT_ERROR.name(), e.getErrorType());
        }
    }
}
