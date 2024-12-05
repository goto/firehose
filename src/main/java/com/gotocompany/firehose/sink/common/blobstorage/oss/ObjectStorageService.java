package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import com.gotocompany.firehose.config.OSSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

public class ObjectStorageService implements BlobStorage {
    private final OSS ossClient;
    private final String bucketName;
    private final String directoryPrefix;

    public ObjectStorageService(OSSConfig config) {
        this.ossClient = new OSSClientBuilder()
                .build(config.getOSSEndpoint(),
                        config.getOSSAccessKeyId(),
                        config.getOSSAccessKeySecret());
        this.bucketName = config.getOSSBucketName();
        this.directoryPrefix = config.getOSSDirectoryPrefix();
    }

    @Override
    public void store(String objectName, String localFilePath) throws BlobStorageException {
        try {
            String fullPath = Paths.get(directoryPrefix, objectName).toString();
            File file = new File(localFilePath);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.length());
            
            try (FileInputStream input = new FileInputStream(file)) {
                ossClient.putObject(bucketName, fullPath, input, metadata);
            }
        } catch (OSSException e) {
            throw new BlobStorageException(e.getErrorCode(), "OSS Upload failed", e);
        } catch (IOException e) {
            throw new BlobStorageException("IO_ERROR", "Failed to read local file", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        try {
            String fullPath = Paths.get(directoryPrefix, objectName).toString();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            ossClient.putObject(bucketName, fullPath, content);
        } catch (OSSException e) {
            throw new BlobStorageException(e.getErrorCode(), "OSS Upload failed", e);
        }
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}
