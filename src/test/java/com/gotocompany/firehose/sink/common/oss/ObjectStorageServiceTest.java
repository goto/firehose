package com.gotocompany.firehose.sink.common.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ObjectStorageServiceTest {

    @Mock
    private OSS ossClient;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private OSSConfig config;
    private ObjectStorageService objectStorage;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "test-prefix");
        }});
        objectStorage = new ObjectStorageService(config);
    }

    @Test
    public void shouldStoreByteArrayWithPrefix() throws BlobStorageException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldStoreByteArrayWithoutPrefix() throws BlobStorageException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldStoreFileWithPrefix() throws BlobStorageException, IOException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        File tempFile = temporaryFolder.newFile("test.txt");
        Files.write(tempFile.toPath(), "test content".getBytes());

        service.store("test.txt", tempFile.getAbsolutePath());

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/test.txt"), any(File.class));
    }

    @Test
    public void shouldStoreFileWithoutPrefix() throws BlobStorageException, IOException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        File tempFile = temporaryFolder.newFile("test.txt");
        Files.write(tempFile.toPath(), "test content".getBytes());

        service.store("test.txt", tempFile.getAbsolutePath());

        verify(ossClient).putObject(eq("TestBucket"), eq("test.txt"), any(File.class));
    }

    @Test
    public void shouldHandleOSSExceptionForByteArray() throws BlobStorageException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        
        doThrow(new OSSException("test error", "ErrorCode", "RequestId", "HostId", "RawMessage"))
            .when(ossClient).putObject(any(), any(), any(ByteArrayInputStream.class), any(ObjectMetadata.class));

        service.store("test.txt", content);
    }

    @Test
    public void shouldHandleOSSExceptionForFile() throws BlobStorageException, IOException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        File tempFile = temporaryFolder.newFile("test.txt");
        Files.write(tempFile.toPath(), "test content".getBytes());

        doThrow(new OSSException("test error", "ErrorCode", "RequestId", "HostId", "RawMessage"))
            .when(ossClient).putObject(any(), any(), any(File.class));

        service.store("test.txt", tempFile.getAbsolutePath());
    }

    @Test
    public void shouldHandleEmptyContent() throws BlobStorageException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = new byte[0];
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleNullPrefix() throws BlobStorageException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", null);
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleEmptyPrefix() throws BlobStorageException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "");
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleSpecialCharactersInObjectName() throws BlobStorageException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test/special#$@!.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/test/special#$@!.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleSpecialCharactersInPrefix() throws BlobStorageException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "test/special#$@!");
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test/special#$@!/test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleLongObjectNames() throws BlobStorageException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        String longName = "a".repeat(1000) + ".txt";
        service.store(longName, content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/" + longName), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleLongPrefixes() throws BlobStorageException {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "a".repeat(1000));
        }});
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("a".repeat(1000) + "/test.txt"), 
            any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void shouldHandleNonExistentFile() throws BlobStorageException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        service.store("test.txt", "/non/existent/path/test.txt");
    }

    @Test
    public void shouldHandleDirectoryAsFile() throws BlobStorageException, IOException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        File tempDir = temporaryFolder.newFolder("testDir");
        service.store("test.txt", tempDir.getAbsolutePath());
    }

    @Test
    public void shouldSetCorrectContentLength() throws BlobStorageException {
        ObjectStorageService service = new ObjectStorageService(config, ossClient);
        byte[] content = "test content".getBytes();
        service.store("test.txt", content);

        verify(ossClient).putObject(eq("TestBucket"), eq("test-prefix/test.txt"), 
            any(ByteArrayInputStream.class), argThat(metadata -> 
                metadata.getContentLength() == content.length));
    }
}
