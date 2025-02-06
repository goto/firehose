package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.TencentCredentialManager;
import com.gotocompany.firehose.sink.common.blobstorage.cos.error.COSErrorType;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.BucketReplicationConfiguration;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.ReplicationRule;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CloudObjectStorageTest {

    @Mock
    private COSClient cosClient;

    @Mock
    private CloudObjectStorageConfig config;

    @Mock
    private TencentCredentialManager credentialManager;

    private CloudObjectStorage cloudObjectStorage;
    private Path tempDir;

    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "test/object.txt";
    private static final String REGION = "ap-singapore";
    private static final String SECRET_ID = "test-secret-id";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String APP_ID = "test-app-id";
    private static final int MAX_RETRIES = 3;
    private static final long CONNECTION_TIMEOUT = 5000L;
    private static final long SOCKET_TIMEOUT = 5000L;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        // Mock config
        when(config.getCosRegion()).thenReturn(REGION);
        when(config.getCosBucketName()).thenReturn(BUCKET_NAME);
        when(config.getCosRetryMaxAttempts()).thenReturn(MAX_RETRIES);
        when(config.getCosConnectionTimeoutMS()).thenReturn(CONNECTION_TIMEOUT);
        when(config.getCosSocketTimeoutMS()).thenReturn(SOCKET_TIMEOUT);
        when(config.getCosSecretId()).thenReturn(SECRET_ID);
        when(config.getCosSecretKey()).thenReturn(SECRET_KEY);
        when(config.getCosAppId()).thenReturn(APP_ID);

        // Mock credentials
        COSCredentials credentials = new BasicCOSCredentials(SECRET_ID, SECRET_KEY);
        when(credentialManager.getCredentials()).thenReturn(credentials);

        // Mock COS client
        when(cosClient.doesBucketExist(anyString())).thenReturn(true);
        when(cosClient.putObject(anyString(), anyString(), any(ByteArrayInputStream.class), any(ObjectMetadata.class))).thenReturn(null);
        when(cosClient.putObject(anyString(), anyString(), any(File.class))).thenReturn(null);

        tempDir = Files.createTempDirectory("cos-test");

        // Create CloudObjectStorage instance with mocked dependencies
        cloudObjectStorage = new CloudObjectStorage(config, credentialManager, cosClient);
    }

    @Test
    public void shouldSuccessfullyInitializeWhenBucketExists() {
        verify(cosClient).doesBucketExist(BUCKET_NAME);
    }

    @Test
    public void shouldThrowExceptionWhenBucketDoesNotExist() {
        when(cosClient.doesBucketExist(BUCKET_NAME)).thenReturn(false);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldThrowExceptionOnCosServiceException() {
        CosServiceException serviceException = new CosServiceException("Service error");
        when(cosClient.doesBucketExist(BUCKET_NAME)).thenThrow(serviceException);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldThrowExceptionOnCosClientException() {
        when(cosClient.doesBucketExist(BUCKET_NAME)).thenThrow(new CosClientException("Client error"));
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldHandleNullBucketName() {
        when(config.getCosBucketName()).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldHandleEmptyBucketName() {
        when(config.getCosBucketName()).thenReturn("");
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldHandleNullRegion() {
        when(config.getCosRegion()).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldHandleEmptyRegion() {
        when(config.getCosRegion()).thenReturn("");
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.checkBucket());
    }

    @Test
    public void shouldStoreFileSuccessfully() throws BlobStorageException, IOException {
        Path tempFile = tempDir.resolve("test.txt");
        Files.write(tempFile, "test content".getBytes(StandardCharsets.UTF_8));

        cloudObjectStorage.store(OBJECT_KEY, tempFile.toString());
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), eq(tempFile.toFile()));
    }

    @Test
    public void shouldHandleNonExistentFile() {
        String nonExistentFile = tempDir.resolve("nonexistent.txt").toString();
        assertThrows(BlobStorageException.class, () -> cloudObjectStorage.store(OBJECT_KEY, nonExistentFile));
    }

    @Test
    public void shouldHandleDirectoryAsFile() throws IOException {
        Path directory = tempDir.resolve("testdir");
        Files.createDirectory(directory);
        assertThrows(BlobStorageException.class, () -> cloudObjectStorage.store(OBJECT_KEY, directory.toString()));
    }

    @Test
    public void shouldHandleEmptyFile() throws BlobStorageException, IOException {
        Path emptyFile = tempDir.resolve("empty.txt");
        Files.createFile(emptyFile);
        cloudObjectStorage.store(OBJECT_KEY, emptyFile.toString());
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), eq(emptyFile.toFile()));
    }

    @Test
    public void shouldHandleLargeFile() throws BlobStorageException, IOException {
        Path largeFile = tempDir.resolve("large.txt");
        byte[] content = new byte[10 * 1024 * 1024];
        Files.write(largeFile, content);
        cloudObjectStorage.store(OBJECT_KEY, largeFile.toString());
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), eq(largeFile.toFile()));
    }

    @Test
    public void shouldStoreByteArraySuccessfully() throws BlobStorageException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        cloudObjectStorage.store(OBJECT_KEY, content);
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleEmptyByteArray() throws BlobStorageException {
        byte[] content = new byte[0];
        cloudObjectStorage.store(OBJECT_KEY, content);
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleLargeByteArray() throws BlobStorageException {
        byte[] content = new byte[10 * 1024 * 1024];
        cloudObjectStorage.store(OBJECT_KEY, content);
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleSpecialCharactersInObjectName() throws BlobStorageException {
        String objectKey = "test/特殊字符/!@#$%^&*()/🔑🗝️.txt";
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        cloudObjectStorage.store(objectKey, content);
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(objectKey), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleVeryLongObjectName() throws BlobStorageException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("very/long/path/");
        }
        String objectKey = sb.toString() + "file.txt";
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        cloudObjectStorage.store(objectKey, content);
        verify(cosClient).putObject(eq(BUCKET_NAME), eq(objectKey), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleNullObjectName() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store(null, content));
    }

    @Test
    public void shouldHandleEmptyObjectName() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store("", content));
    }

    @Test
    public void shouldHandleNullContent() {
        assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store(OBJECT_KEY, (byte[]) null));
    }

    @Test
    public void shouldHandleServiceErrorWithRetry() throws BlobStorageException {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(500);
        when(cosClient.putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
                .thenThrow(serviceException)
                .thenThrow(serviceException)
                .thenReturn(null);
        cloudObjectStorage.store(OBJECT_KEY, content);
        verify(cosClient, times(3)).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleClientErrorWithRetry() throws BlobStorageException {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosClientException clientException = new CosClientException("Client error");
        when(cosClient.putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
                .thenThrow(clientException)
                .thenThrow(clientException)
                .thenReturn(null);
        cloudObjectStorage.store(OBJECT_KEY, content);
        verify(cosClient, times(3)).putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleInterruptedThread() {
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        CosServiceException serviceException = new CosServiceException("Service error");
        serviceException.setStatusCode(503);
        when(cosClient.putObject(eq(BUCKET_NAME), eq(OBJECT_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
            .thenThrow(serviceException);

        Thread.currentThread().interrupt();
        try {
            cloudObjectStorage.store(OBJECT_KEY, content);
            fail("Expected BlobStorageException");
        } catch (BlobStorageException e) {
            assertTrue(Thread.interrupted());
            assertEquals(COSErrorType.DEFAULT_ERROR.name(), e.getErrorType());
        }
    }

    @Test
    public void shouldLogRetentionPolicyWhenAvailable() {
        BucketReplicationConfiguration replicationConfig = new BucketReplicationConfiguration();
        ReplicationRule rule = new ReplicationRule();
        rule.setStatus("Enabled");
        replicationConfig.setRules(Collections.singletonList(rule));
        when(cosClient.getBucketReplicationConfiguration(BUCKET_NAME)).thenReturn(replicationConfig);
        verify(cosClient).getBucketReplicationConfiguration(BUCKET_NAME);
    }
}
