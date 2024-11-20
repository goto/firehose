package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.PutObjectRequest;
import com.gotocompany.firehose.config.ObjectStorageServiceConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Optional;

@Slf4j
public class ObjectStorageService implements BlobStorage {

    private final OSS oss;
    private final ObjectStorageServiceConfig objectStorageServiceConfig;

    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig) {
        this(objectStorageServiceConfig, initializeOss(objectStorageServiceConfig));
    }

    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig, OSS oss) {
        this.oss = oss;
        this.objectStorageServiceConfig = objectStorageServiceConfig;
        checkBucket();
    }

    private static OSS initializeOss(ObjectStorageServiceConfig objectStorageServiceConfig) {
        ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
        clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
        return OSSClientBuilder.create()
                .endpoint(objectStorageServiceConfig.getOssEndpoint())
                .region(objectStorageServiceConfig.getOssRegion())
                .credentialsProvider(new DefaultCredentialProvider(objectStorageServiceConfig.getOssAccessId(),
                        objectStorageServiceConfig.getOssAccessKey()))
                .clientConfiguration(clientBuilderConfiguration)
                .build();
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                objectStorageServiceConfig.getOssBucketName(),
                buildObjectPath(objectName),
                new File(filePath)
        );
        putObject(putObjectRequest);
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                objectStorageServiceConfig.getOssBucketName(),
                buildObjectPath(objectName),
                new ByteArrayInputStream(content)
        );
        putObject(putObjectRequest);
    }

    private void putObject(PutObjectRequest putObjectRequest) throws BlobStorageException {
        try {
            oss.putObject(putObjectRequest);
        } catch (ClientException e) {
            log.error("Failed to put object to OSS", e);
            throw new BlobStorageException("client_error", e.getMessage(), e);
        } catch (OSSException e) {
            log.error("Failed to put object to OSS requestID:{} hostID:{}", e.getRequestId(), e.getHostId());
            throw new BlobStorageException(e.getErrorCode(), e.getErrorMessage(), e);
        }
    }

    private String buildObjectPath(String objectName) {
        return Optional.ofNullable(objectStorageServiceConfig.getOssDirectoryPrefix())
                .map(prefix -> prefix + "/" + objectName)
                .orElse(objectName);
    }

    private void checkBucket() {
        BucketList bucketList = oss.listBuckets(new ListBucketsRequest(objectStorageServiceConfig.getOssBucketName(),
                null, 1));
        if (bucketList.getBucketList().isEmpty()) {
            log.error("Bucket does not exist:{}", objectStorageServiceConfig.getOssBucketName());
            log.error("Please create OSS bucket before running firehose: {}", objectStorageServiceConfig.getOssBucketName());
            throw new IllegalArgumentException("Bucket does not exist");
        }
    }

}