package com.gotocompany.firehose.sink.common.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
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
import java.nio.file.Paths;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ObjectStorageServiceTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private OSSConfig config;
    private OSS ossClient;
    private ObjectStorageService objectStorageService;

    @Before
    public void setup() {
        ossClient = mock(OSS.class);
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key-id");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-key-secret");
            put("SOME_TYPE_OSS_DIRECTORY_PREFIX", "test-prefix");
        }});
        objectStorageService = new ObjectStorageService(config) {
            @Override
            protected OSS createOSSClient(String endpoint, String accessKeyId, String accessKeySecret) {
                return ossClient;
            }
        };
    }

    @Test
    public void shouldStoreByteArraySuccessfully() throws BlobStorageException {
        byte[] content = "test content".getBytes();
        objectStorageService.store("test.txt", content);

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test-prefix/test.txt"),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldStoreFileSuccessfully() throws BlobStorageException, IOException {
        File tempFile = temporaryFolder.newFile("test.txt");
        Files.write(tempFile.toPath(), "test content".getBytes());

        objectStorageService.store("test.txt", tempFile.getAbsolutePath());

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test-prefix/test.txt"),
            eq(tempFile)
        );
    }

    @Test
    public void shouldHandleOSSException() throws BlobStorageException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        OSSException ossException = new OSSException("Test error", "ErrorCode", "RequestId", "HostId");
        doThrow(ossException).when(ossClient).putObject(
            any(String.class),
            any(String.class),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );

        objectStorageService.store("test.txt", "test content".getBytes());
    }

    @Test
    public void shouldHandleEmptyContent() throws BlobStorageException {
        byte[] emptyContent = new byte[0];
        objectStorageService.store("empty.txt", emptyContent);

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test-prefix/empty.txt"),
            any(ByteArrayInputStream.class),
            argThat(metadata -> metadata.getContentLength() == 0)
        );
    }

    @Test
    public void shouldHandleNullDirectoryPrefix() {
        config = ConfigFactory.create(OSSConfig.class, new HashMap<Object, Object>() {{
            put("OSS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_OSS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_OSS_ENDPOINT", "test-endpoint");
            put("SOME_TYPE_OSS_ACCESS_KEY_ID", "test-key-id");
            put("SOME_TYPE_OSS_ACCESS_KEY_SECRET", "test-key-secret");
        }});

        objectStorageService = new ObjectStorageService(config) {
            @Override
            protected OSS createOSSClient(String endpoint, String accessKeyId, String accessKeySecret) {
                return ossClient;
            }
        };

        byte[] content = "test content".getBytes();
        objectStorageService.store("test.txt", content);

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test.txt"),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldHandleSpecialCharactersInObjectName() throws BlobStorageException {
        byte[] content = "test content".getBytes();
        objectStorageService.store("special/chars!@#$%^&*.txt", content);

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test-prefix/special/chars!@#$%^&*.txt"),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldHandleNonExistentFile() throws BlobStorageException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        objectStorageService.store("test.txt", "/non/existent/path/test.txt");
    }

    @Test
    public void shouldHandleDirectoryAsFile() throws BlobStorageException, IOException {
        thrown.expect(BlobStorageException.class);
        thrown.expectMessage("OSS Upload failed");

        File tempDir = temporaryFolder.newFolder("testDir");
        objectStorageService.store("test.txt", tempDir.getAbsolutePath());
    }

    @Test
    public void shouldHandleLongObjectName() throws BlobStorageException {
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longName.append("a");
        }
        longName.append(".txt");

        byte[] content = "test content".getBytes();
        objectStorageService.store(longName.toString(), content);

        verify(ossClient).putObject(
            eq("TestBucket"),
            eq("test-prefix/" + longName),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }

    @Test
    public void shouldHandleMultipleUploads() throws BlobStorageException {
        byte[] content = "test content".getBytes();
        objectStorageService.store("test1.txt", content);
        objectStorageService.store("test2.txt", content);
        objectStorageService.store("test3.txt", content);

        verify(ossClient, times(3)).putObject(
            eq("TestBucket"),
            any(String.class),
            any(ByteArrayInputStream.class),
            any(ObjectMetadata.class)
        );
    }
}
