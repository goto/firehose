package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectRequest;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;

import java.io.ByteArrayInputStream;
import java.io.File;

public class OSS implements BlobStorage {
    private final OSS ossClient;
    private final String bucketName;
    private final String directoryPrefix;
    private final int maxRetries;

    public OSS(OSSConfig ossConfig) {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setMaxErrorRetry(ossConfig.getOSSRetryMaxAttempts());
        conf.setConnectionTimeout(ossConfig.getOSSRetryTotalTimeoutMS().intValue());
        conf.setSocketTimeout(ossConfig.getOSSRetryTotalTimeoutMS().intValue());

        this.ossClient = new OSSClientBuilder().build(
                ossConfig.getOSSEndpoint(),
                ossConfig.getOSSAccessKeyId(),
                ossConfig.getOSSAccessKeySecret(),
                conf
        );
        this.bucketName = ossConfig.getOSSBucketName();
        this.directoryPrefix = ossConfig.getOSSDirectoryPrefix();
        this.maxRetries = ossConfig.getOSSRetryMaxAttempts();
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        try {
            String fullPath = directoryPrefix.isEmpty() ? objectName : directoryPrefix + "/" + objectName;
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fullPath, new File(filePath));
            ossClient.putObject(putObjectRequest);
        } catch (OSSException e) {
            throw new BlobStorageException("Failed to upload file to OSS", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        try {
            String fullPath = directoryPrefix.isEmpty() ? objectName : directoryPrefix + "/" + objectName;
            ossClient.putObject(bucketName, fullPath, new ByteArrayInputStream(content));
        } catch (OSSException e) {
            throw new BlobStorageException("Failed to upload content to OSS", e);
        }
    }
}
