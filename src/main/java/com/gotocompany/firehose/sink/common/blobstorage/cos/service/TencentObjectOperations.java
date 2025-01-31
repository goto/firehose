package com.gotocompany.firehose.sink.common.blobstorage.cos.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class TencentObjectOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentObjectOperations.class);
    private final COSClient cosClient;
    private final CloudObjectStorageConfig config;

    public TencentObjectOperations(COSClient cosClient, CloudObjectStorageConfig config) {
        this.cosClient = cosClient;
        this.config = config;
    }

    public void uploadObject(String objectKey, String content) throws BlobStorageException {
        try {
            cosClient.putObject(config.getCosBucketName(), objectKey, content);
            LOGGER.info("Successfully uploaded object: {}", objectKey);
        } catch (CosServiceException e) {
            LOGGER.error("COS service error while uploading: {}", objectKey);
            throw new BlobStorageException(e.getErrorCode(), e.getMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("COS client error while uploading: {}", objectKey);
            throw new BlobStorageException(e.getErrorCode(), e.getMessage(), e);
        }
    }

    public String buildObjectPath(String objectName) {
        String prefix = config.getCosDirectoryPrefix();
        if (prefix != null && !prefix.endsWith("/") && objectName.startsWith("/")) {
            prefix += "/";
        }
        return prefix == null || prefix.isEmpty() ?
                objectName :
                Paths.get(prefix, objectName).toString();
    }
}