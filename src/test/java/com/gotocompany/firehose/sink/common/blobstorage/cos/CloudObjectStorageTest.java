package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.BucketReplicationConfiguration;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.ReplicationRule;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Response;
import com.tencent.cloud.Credentials;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.TreeMap;
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

    private CloudObjectStorage cloudObjectStorage;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        when(config.getCosRegion()).thenReturn("ap-singapore");
        when(config.getCosBucketName()).thenReturn("test-bucket");
        when(config.getCosRetryMaxAttempts()).thenReturn(3);
        when(config.getCosConnectionTimeoutMS()).thenReturn(5000L);
        when(config.getCosSocketTimeoutMS()).thenReturn(5000L);
        when(config.getCosSecretId()).thenReturn("test-secret-id");
        when(config.getCosSecretKey()).thenReturn("test-secret-key");
        when(cosClient.doesBucketExist(anyString())).thenReturn(true);

        tempDir = Files.createTempDirectory("cos-test");
    }

    @Test
    public void shouldSuccessfullyInitializeWhenBucketExists() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldThrowExceptionWhenBucketDoesNotExist() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(cosClient.doesBucketExist(anyString())).thenReturn(false);
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldThrowExceptionOnCosServiceException() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(cosClient.doesBucketExist(anyString()))
                .thenThrow(new CosServiceException("Service error"));
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldThrowExceptionOnCosClientException() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(cosClient.doesBucketExist(anyString()))
                .thenThrow(new CosClientException("Client error"));
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldHandleNullBucketName() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosBucketName()).thenReturn(null);
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldHandleEmptyBucketName() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosBucketName()).thenReturn("");
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldHandleNullRegion() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosRegion()).thenReturn(null);
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldHandleEmptyRegion() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosRegion()).thenReturn("");
            assertThrows(IllegalArgumentException.class, () -> new CloudObjectStorage(config));
        }
    }

    @Test
    public void shouldHandleNullRetryAttempts() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosRetryMaxAttempts()).thenReturn(null);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldHandleZeroRetryAttempts() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosRetryMaxAttempts()).thenReturn(0);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldHandleNegativeRetryAttempts() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosRetryMaxAttempts()).thenReturn(-1);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldHandleNullTimeoutValues() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosConnectionTimeoutMS()).thenReturn(null);
            when(config.getCosSocketTimeoutMS()).thenReturn(null);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldHandleZeroTimeoutValues() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosConnectionTimeoutMS()).thenReturn(0L);
            when(config.getCosSocketTimeoutMS()).thenReturn(0L);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldHandleNegativeTimeoutValues() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(config.getCosConnectionTimeoutMS()).thenReturn(-1L);
            when(config.getCosSocketTimeoutMS()).thenReturn(-1L);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).doesBucketExist("test-bucket");
        }
    }

    @Test
    public void shouldLogRetentionPolicyWhenAvailable() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            BucketReplicationConfiguration replicationConfig = mock(BucketReplicationConfiguration.class);
            ReplicationRule rule = mock(ReplicationRule.class);
            when(rule.getID()).thenReturn("rule1");
            when(rule.getStatus()).thenReturn("Enabled");
            when(replicationConfig.getRules()).thenReturn(Collections.singletonList(rule));
            when(cosClient.getBucketReplicationConfiguration(anyString())).thenReturn(replicationConfig);

            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).getBucketReplicationConfiguration("test-bucket");
        }
    }

    @Test
    public void shouldHandleNullRetentionPolicy() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(cosClient.getBucketReplicationConfiguration(anyString())).thenReturn(null);
            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).getBucketReplicationConfiguration("test-bucket");
        }
    }

    @Test
    public void shouldHandleEmptyRetentionPolicy() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            BucketReplicationConfiguration replicationConfig = mock(BucketReplicationConfiguration.class);
            when(replicationConfig.getRules()).thenReturn(Collections.emptyList());
            when(cosClient.getBucketReplicationConfiguration(anyString())).thenReturn(replicationConfig);

            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).getBucketReplicationConfiguration("test-bucket");
        }
    }

    @Test
    public void shouldHandleNullRulesInRetentionPolicy() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            BucketReplicationConfiguration replicationConfig = mock(BucketReplicationConfiguration.class);
            when(replicationConfig.getRules()).thenReturn(null);
            when(cosClient.getBucketReplicationConfiguration(anyString())).thenReturn(replicationConfig);

            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).getBucketReplicationConfiguration("test-bucket");
        }
    }

    @Test
    public void shouldHandleExceptionInRetentionPolicyFetch() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            when(cosClient.getBucketReplicationConfiguration(anyString()))
                .thenThrow(new CosServiceException("Failed to fetch policy"));

            cloudObjectStorage = new CloudObjectStorage(config);
            verify(cosClient).getBucketReplicationConfiguration("test-bucket");
        }
    }

    @Test
    public void shouldStoreFileSuccessfully() throws BlobStorageException, IOException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            Path tempFile = tempDir.resolve("test.txt");
            Files.write(tempFile, "test content".getBytes(StandardCharsets.UTF_8));

            String objectName = "test/object.txt";
            cloudObjectStorage.store(objectName, tempFile.toString());
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), eq(tempFile.toFile()));
        }
    }

    @Test
    public void shouldHandleNonExistentFile() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String nonExistentFile = tempDir.resolve("nonexistent.txt").toString();
            assertThrows(BlobStorageException.class, () -> 
                cloudObjectStorage.store("test/object.txt", nonExistentFile));
        }
    }

    @Test
    public void shouldHandleDirectoryAsFile() throws IOException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            Path directory = tempDir.resolve("testdir");
            Files.createDirectory(directory);

            assertThrows(BlobStorageException.class, () ->
                cloudObjectStorage.store("test/object.txt", directory.toString()));
        }
    }

    @Test
    public void shouldHandleEmptyFile() throws BlobStorageException, IOException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            Path emptyFile = tempDir.resolve("empty.txt");
            Files.createFile(emptyFile);

            String objectName = "test/empty.txt";
            cloudObjectStorage.store(objectName, emptyFile.toString());
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), eq(emptyFile.toFile()));
        }
    }

    @Test
    public void shouldHandleLargeFile() throws BlobStorageException, IOException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            Path largeFile = tempDir.resolve("large.txt");
            
            byte[] content = new byte[10 * 1024 * 1024];
            Files.write(largeFile, content);

            String objectName = "test/large.txt";
            cloudObjectStorage.store(objectName, largeFile.toString());
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), eq(largeFile.toFile()));
        }
    }

    @Test
    public void shouldStoreByteArraySuccessfully() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/object.txt";
            byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
            cloudObjectStorage.store(objectName, content);
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleEmptyByteArray() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/empty.txt";
            byte[] content = new byte[0];
            cloudObjectStorage.store(objectName, content);
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleLargeByteArray() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/large.txt";
            byte[] content = new byte[10 * 1024 * 1024];
            cloudObjectStorage.store(objectName, content);
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleSpecialCharactersInObjectName() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/特殊字符/!@#$%^&*()/🔑🗝️.txt";
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);
            cloudObjectStorage.store(objectName, content);
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleVeryLongObjectName() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("very/long/path/");
            }
            sb.append("file.txt");
            String objectName = sb.toString();
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);
            cloudObjectStorage.store(objectName, content);
            verify(cosClient).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleNullObjectName() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);
            assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store(null, content));
        }
    }

    @Test
    public void shouldHandleEmptyObjectName() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);
            assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store("", content));
        }
    }

    @Test
    public void shouldHandleNullContent() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            assertThrows(IllegalArgumentException.class, () -> cloudObjectStorage.store("test.txt", (byte[]) null));
        }
    }

    @Test
    public void shouldHandleServiceErrorWithRetry() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/object.txt";
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);

            CosServiceException serviceException = new CosServiceException("Service error");
            serviceException.setStatusCode(500);

            when(cosClient.putObject(anyString(), anyString(), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
                .thenThrow(serviceException)
                .thenThrow(serviceException)
                .thenReturn(null);

            cloudObjectStorage.store(objectName, content);
            verify(cosClient, times(3)).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleClientErrorWithRetry() throws BlobStorageException {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/object.txt";
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);

            when(cosClient.putObject(anyString(), anyString(), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
                .thenThrow(new CosClientException("Network error"))
                .thenThrow(new CosClientException("Network error"))
                .thenReturn(null);

            cloudObjectStorage.store(objectName, content);
            verify(cosClient, times(3)).putObject(eq("test-bucket"), eq(objectName), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
        }
    }

    @Test
    public void shouldHandleInterruptedThread() {
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "test-tmp-secret-id";
            mockResponse.credentials.tmpSecretKey = "test-tmp-secret-key";
            mockResponse.credentials.sessionToken = "test-session-token";
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                .thenReturn(mockResponse);

            cloudObjectStorage = new CloudObjectStorage(config);
            String objectName = "test/object.txt";
            byte[] content = "test".getBytes(StandardCharsets.UTF_8);

            when(cosClient.putObject(anyString(), anyString(), any(ByteArrayInputStream.class), any(ObjectMetadata.class)))
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return null;
                });

            assertThrows(BlobStorageException.class, () -> cloudObjectStorage.store(objectName, content));
            assertTrue(Thread.interrupted());
        }
    }
} 