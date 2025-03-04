package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.SessionTokenGenerator;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.TokenLifecycleManager;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSSessionCredentials;
import com.qcloud.cos.exception.CosServiceException;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.HashMap;

import static org.mockito.Mockito.when;

public class CloudObjectStorageTest {
    private CloudObjectStorageConfig cloudObjectStorageConfig;
    private COSClient cosClient;
    private SessionTokenGenerator sessionTokenGenerator;
    private TokenLifecycleManager tokenLifecycleManager;

    public void init() {
        cosClient = Mockito.mock(COSClient.class);
        sessionTokenGenerator = Mockito.mock(SessionTokenGenerator.class);
        tokenLifecycleManager = new TokenLifecycleManager(cloudObjectStorageConfig, cosClient, sessionTokenGenerator);
        COSSessionCredentials mockCredentials = new BasicSessionCredentials("", "", "");
        when(sessionTokenGenerator.getCOSCredentials()).thenReturn(mockCredentials);
    }

    @Test
    public void shouldCallStorageWithPrefix() throws BlobStorageException {
        cloudObjectStorageConfig = ConfigFactory.create(CloudObjectStorageConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_DIRECTORY_PREFIX", "prefix");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        CloudObjectStorage cos = new CloudObjectStorage(cloudObjectStorageConfig, cosClient,
                sessionTokenGenerator, tokenLifecycleManager);
        cos.store("test", new byte[]{});
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cloudObjectStorageConfig.getCOSBucketName(), "prefix/test", "");
    }

    @Test
    public void shouldCallStorageWithoutPrefix() throws BlobStorageException {
        cloudObjectStorageConfig = ConfigFactory.create(CloudObjectStorageConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        CloudObjectStorage cos = new CloudObjectStorage(cloudObjectStorageConfig, cosClient,
                sessionTokenGenerator, tokenLifecycleManager);
        cos.store("test", new byte[]{});
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cloudObjectStorageConfig.getCOSBucketName(), "test", "");
    }

    @Test
    public void shouldThrowBlobStorageException() {
        cloudObjectStorageConfig = ConfigFactory.create(CloudObjectStorageConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        CloudObjectStorage cos = new CloudObjectStorage(cloudObjectStorageConfig, cosClient,
                sessionTokenGenerator, tokenLifecycleManager);
        CosServiceException e = new CosServiceException("BlobStorageException error was expected");
        e.setErrorCode("testCode");
        Mockito.when(cosClient.putObject(cloudObjectStorageConfig.getCOSBucketName(), "test", "")).
                thenThrow(e);
        BlobStorageException thrown = Assertions.assertThrows(BlobStorageException.class,
                () -> cos.store("test", new byte[]{}),
                "Expected throw BlobStorageException, actual not");
        Assert.assertEquals(new BlobStorageException(e.getErrorCode(), e.getMessage(), e), thrown);
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cloudObjectStorageConfig.getCOSBucketName(), "test", "");
    }
}
