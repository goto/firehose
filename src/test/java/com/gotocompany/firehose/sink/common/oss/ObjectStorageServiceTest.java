package com.gotocompany.firehose.sink.common.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.PutObjectRequest;
import com.gotocompany.firehose.config.ObjectStorageServiceConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ObjectStorageServiceTest {

    @Test
    public void shouldStoreObjectGivenFilePath() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        Mockito.when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        Mockito.when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        Mockito.when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        Mockito.when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        Mockito.when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        Mockito.when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        Mockito.when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "filePath");

        Mockito.verify(oss, Mockito.times(1))
                .putObject(argumentCaptor.capture());
        assertEquals("bucket_name", argumentCaptor.getValue().getBucketName());
        assertEquals("dir_prefix/objectName", argumentCaptor.getValue().getKey());
        assertEquals(new File("filePath"), argumentCaptor.getValue().getFile());
    }

    @Test
    public void shouldStoreObjectGivenFileContent() throws BlobStorageException, IOException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        Mockito.when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        Mockito.when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        Mockito.when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        Mockito.when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        Mockito.when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        Mockito.when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        Mockito.when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        Mockito.when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(oss, Mockito.times(1))
                .putObject(argumentCaptor.capture());
        assertEquals("bucket_name", argumentCaptor.getValue().getBucketName());
        assertEquals("dir_prefix/objectName", argumentCaptor.getValue().getKey());
        InputStream inputStream = argumentCaptor.getValue().getInputStream();
        assertEquals("content", getContent(inputStream));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenGivenBucketIsNotExists() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        Mockito.when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        Mockito.when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        Mockito.when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        Mockito.when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        Mockito.when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        Mockito.when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(new ArrayList<>());
        Mockito.when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldWrapToBlobStorageExceptionWhenClientExceptionIsThrown() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        Mockito.when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        Mockito.when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        Mockito.when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        Mockito.when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        Mockito.when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        Mockito.when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        Mockito.when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new ClientException("client_error"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        Mockito.when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldWrapToBlobStorageExceptionWhenOSSExceptionIsThrown() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        Mockito.when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        Mockito.when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        Mockito.when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        Mockito.when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        Mockito.when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        Mockito.when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        Mockito.when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new OSSException("server is down"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        Mockito.when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    private static String getContent(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }

}
