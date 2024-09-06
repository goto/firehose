package com.gotocompany.firehose.sink.common.cos;

import com.gotocompany.firehose.config.COSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CredentialProvider;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CredentialRefresher;
import com.gotocompany.firehose.sink.common.blobstorage.cos.TencentCloudStorage;
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

public class TencentCloudStorageTest {
    private COSConfig cosConfig;
    private COSClient cosClient;
    private CredentialProvider credentialProvider;
    private CredentialRefresher credentialRefresher;

    public void init() {
        cosClient = Mockito.mock(COSClient.class);
        credentialProvider = Mockito.mock(CredentialProvider.class);
        credentialRefresher = new CredentialRefresher(cosConfig, cosClient, credentialProvider);
        COSSessionCredentials mockCredentials = new BasicSessionCredentials("", "", "");
        when(credentialProvider.getCOSCredentials()).thenReturn(mockCredentials);
    }

    @Test
    public void shouldCallStorageWithPrefix() throws BlobStorageException {
        cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_DIRECTORY_PREFIX", "prefix");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        TencentCloudStorage cos = new TencentCloudStorage(cosConfig, cosClient,
                credentialProvider, credentialRefresher);
        cos.store("test", new byte[]{});
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cosConfig.getCOSBucketName(), "prefix/test", "");
    }

    @Test
    public void shouldCallStorageWithoutPrefix() throws BlobStorageException {
        cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        TencentCloudStorage cos = new TencentCloudStorage(cosConfig, cosClient,
                credentialProvider, credentialRefresher);
        cos.store("test", new byte[]{});
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cosConfig.getCOSBucketName(), "test", "");
    }

    @Test
    public void shouldThrowBlobStorageException() {
        cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        init();
        TencentCloudStorage cos = new TencentCloudStorage(cosConfig, cosClient,
                credentialProvider, credentialRefresher);
        CosServiceException e = new CosServiceException("BlobStorageException error was expected");
        e.setErrorCode("testCode");
        Mockito.when(cosClient.putObject(cosConfig.getCOSBucketName(), "test", "")).
                thenThrow(e);
        BlobStorageException thrown = Assertions.assertThrows(BlobStorageException.class,
                () -> cos.store("test", new byte[]{}),
                "Expected throw BlobStorageException, actual not");
        Assert.assertEquals(new BlobStorageException(e.getErrorCode(), e.getMessage(), e), thrown);
        Mockito.verify(cosClient, Mockito.times(1)).
                putObject(cosConfig.getCOSBucketName(), "test", "");
    }
}
