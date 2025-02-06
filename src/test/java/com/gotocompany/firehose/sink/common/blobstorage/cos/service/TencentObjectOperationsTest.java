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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
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

    @Before
    public void setUp() throws IOException {
        when(config.getCosBucketName()).thenReturn(BUCKET_NAME);
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
            any(ObjectMetadata.class)
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
        operations.uploadObject(OBJECT_KEY, new byte[0]);
        verify(cosClient).putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
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
            any(ObjectMetadata.class)
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
            any(ObjectMetadata.class)
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
            any(ObjectMetadata.class)
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
        when(cosClient.putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        ))
            .thenAnswer(invocation -> {
                Thread.currentThread().interrupt();
                return null;
            });

        assertThrows(BlobStorageException.class, () -> operations.uploadObject(OBJECT_KEY, content));
        assertTrue(Thread.interrupted());
    }

    @Test
    public void shouldHandleAllHttpStatusCodes() {
        int[] statusCodes = {400, 401, 403, 404, 405, 409, 429, 500, 503, 504};
        COSErrorType[] expectedErrors = {
            COSErrorType.BAD_REQUEST,
            COSErrorType.UNAUTHORIZED,
            COSErrorType.FORBIDDEN,
            COSErrorType.NOT_FOUND,
            COSErrorType.METHOD_NOT_ALLOWED,
            COSErrorType.CONFLICT,
            COSErrorType.TOO_MANY_REQUESTS,
            COSErrorType.INTERNAL_SERVER_ERROR,
            COSErrorType.SERVICE_UNAVAILABLE,
            COSErrorType.GATEWAY_TIMEOUT
        };

        for (int i = 0; i < statusCodes.length; i++) {
            CosServiceException exception = new CosServiceException("Service error");
            exception.setStatusCode(statusCodes[i]);

            when(cosClient.putObject(
                eq(BUCKET_NAME),
                eq(OBJECT_KEY),
                any(ByteArrayInputStream.class),
                any(ObjectMetadata.class)
            ))
                .thenThrow(exception);

            try {
                operations.uploadObject(OBJECT_KEY, "test".getBytes(StandardCharsets.UTF_8));
                fail("Expected BlobStorageException");
            } catch (BlobStorageException e) {
                assertEquals(expectedErrors[i].name(), e.getErrorType());
            }
        }
    }

    @Test
    public void shouldHandleUnknownHttpStatusCode() {
        CosServiceException exception = new CosServiceException("Service error");
        exception.setStatusCode(599);

        when(cosClient.putObject(
            eq(BUCKET_NAME),
            eq(OBJECT_KEY),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        ))
            .thenThrow(exception);

        try {
            operations.uploadObject(OBJECT_KEY, "test".getBytes(StandardCharsets.UTF_8));
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.DEFAULT_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleNullBucketName() {
        when(config.getCosBucketName()).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> new TencentObjectOperations(cosClient, config));
    }

    @Test
    public void shouldHandleEmptyBucketName() {
        when(config.getCosBucketName()).thenReturn("");
        assertThrows(IllegalArgumentException.class, () -> new TencentObjectOperations(cosClient, config));
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
    public void shouldHandleDeleteNonExistentObject() {
        CosServiceException exception = new CosServiceException("Object not found");
        exception.setStatusCode(404);
        doThrow(exception).when(cosClient).deleteObject(BUCKET_NAME, OBJECT_KEY);

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.NOT_FOUND.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithServiceError() {
        CosServiceException exception = new CosServiceException("Service error");
        exception.setStatusCode(500);
        doThrow(exception).when(cosClient).deleteObject(BUCKET_NAME, OBJECT_KEY);

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.INTERNAL_SERVER_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithClientError() {
        doThrow(new CosClientException("Network error"))
            .when(cosClient).deleteObject(BUCKET_NAME, OBJECT_KEY);

        try {
            operations.deleteObject(OBJECT_KEY);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertEquals(COSErrorType.DEFAULT_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldHandleDeleteWithInterruptedThread() {
        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return null;
        }).when(cosClient).deleteObject(BUCKET_NAME, OBJECT_KEY);

        assertThrows(BlobStorageException.class, () -> operations.deleteObject(OBJECT_KEY));
        assertTrue(Thread.interrupted());
    }
}
