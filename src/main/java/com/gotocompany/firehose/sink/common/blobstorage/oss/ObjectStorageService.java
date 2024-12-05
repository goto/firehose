package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Paths;

public class ObjectStorageService implements BlobStorage {
    private final OSS ossClient;
    private final String bucketName;
    private final String directoryPrefix;

    public ObjectStorageService(OSSConfig config) {
        this.ossClient = createOSSClient(
                config.getOSSEndpoint(),
                config.getOSSAccessKeyId(),
                config.getOSSAccessKeySecret()
        );
        this.bucketName = config.getOSSBucketName();
        this.directoryPrefix = config.getOSSDirectoryPrefix();
    }

    protected OSS createOSSClient(String endpoint, String accessKeyId, String accessKeySecret) {
        return new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    @Override
    public void store(String objectName, String localPath) throws BlobStorageException {
        try {
            String fullPath = Paths.get(directoryPrefix, objectName).toString();
            ossClient.putObject(bucketName, fullPath, new File(localPath));
        } catch (OSSException e) {
            throw new BlobStorageException(e.getErrorCode(), "OSS Upload failed", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        try {
            String fullPath = Paths.get(directoryPrefix, objectName).toString();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            ossClient.putObject(bucketName, fullPath, new ByteArrayInputStream(content), metadata);
        } catch (OSSException e) {
            throw new BlobStorageException(e.getErrorCode(), "OSS Upload failed", e);
        }
    }
}
