package com.gotocompany.firehose.sink.common.blobstorage;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageFactoryTest {

    private Map<String, String> validOssConfig;

    @Mock
    private OSS ossClient;

    @Mock
    private BucketList bucketList;

    @Before
    public void setUp() {
        validOssConfig = new HashMap<>();
        validOssConfig.put("OSS_TYPE_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
        validOssConfig.put("OSS_TYPE_OSS_REGION", "cn-hangzhou");
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_ID", "test-access-id");
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_KEY", "test-access-key");
        validOssConfig.put("OSS_TYPE_OSS_BUCKET_NAME", "test-bucket");
        validOssConfig.put("OSS_TYPE_OSS_DIRECTORY_PREFIX", "test-prefix");

        when(ossClient.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
        when(bucketList.getBucketList()).thenReturn(Collections.singletonList(mock(Bucket.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_NullConfig_ThrowsException() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_EmptyConfig_ThrowsException() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, new HashMap<>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_MissingEndpoint_ThrowsException() {
        validOssConfig.remove("OSS_TYPE_OSS_ENDPOINT");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_MissingRegion_ThrowsException() {
        validOssConfig.remove("OSS_TYPE_OSS_REGION");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_MissingAccessId_ThrowsException() {
        validOssConfig.remove("OSS_TYPE_OSS_ACCESS_ID");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_MissingAccessKey_ThrowsException() {
        validOssConfig.remove("OSS_TYPE_OSS_ACCESS_KEY");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_MissingBucketName_ThrowsException() {
        validOssConfig.remove("OSS_TYPE_OSS_BUCKET_NAME");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }


    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidEndpoint_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_ENDPOINT", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_EmptyAccessId_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_ID", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_EmptyAccessKey_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_KEY", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_EmptyBucketName_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_BUCKET_NAME", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidSocketTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_SOCKET_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_NegativeSocketTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_SOCKET_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidConnectionTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_CONNECTION_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_NegativeConnectionTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_CONNECTION_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidRequestTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_REQUEST_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_NegativeRequestTimeout_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_REQUEST_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidRetryEnabled_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_RETRY_ENABLED", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidMaxRetryAttempts_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_NegativeMaxRetryAttempts_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "-1");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_ZeroMaxRetryAttempts_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "0");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createObjectStorage_InvalidRegion_ThrowsException() {
        validOssConfig.put("OSS_TYPE_OSS_REGION", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }


}