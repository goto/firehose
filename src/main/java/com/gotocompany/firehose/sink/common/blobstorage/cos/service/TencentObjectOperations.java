package com.gotocompany.firehose.sink.common.blobstorage.cos.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.error.COSErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;

public class TencentObjectOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentObjectOperations.class);
    private final COSClient cosClient;
    private final CloudObjectStorageConfig config;

    public TencentObjectOperations(COSClient cosClient, CloudObjectStorageConfig config) {
        this.cosClient = cosClient;
        this.config = config;
    }

    public void uploadObject(String objectKey, byte[] content) throws BlobStorageException {
        String blobPath = String.join("/", config.getCosBucketName(), objectKey);
        LOGGER.info("Attempting to upload object to COS: {}", blobPath);
        
        try {
            cosClient.putObject(config.getCosBucketName(), objectKey, new ByteArrayInputStream(content), null);
            LOGGER.info("Successfully uploaded object to COS: {}", blobPath);
        } catch (CosServiceException e) {
            LOGGER.error("COS service error while uploading {}: {} - {}", 
                blobPath, e.getErrorCode(), e.getErrorMessage());
            COSErrorType errorType = mapServiceError(e);
            throw new BlobStorageException(errorType.name(), e.getErrorMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("COS client error while uploading {}: {}", blobPath, e.getMessage());
            COSErrorType errorType = mapClientError(e);
            throw new BlobStorageException(errorType.name(), "Failed to upload to COS", e);
        }
    }

    private COSErrorType mapServiceError(CosServiceException e) {
        switch (e.getStatusCode()) {
            case 400:
                return COSErrorType.BAD_REQUEST;
            case 401:
                return COSErrorType.UNAUTHORIZED;
            case 403:
                return COSErrorType.FORBIDDEN;
            case 404:
                return COSErrorType.NOT_FOUND;
            case 405:
                return COSErrorType.METHOD_NOT_ALLOWED;
            case 409:
                return COSErrorType.CONFLICT;
            case 429:
                return COSErrorType.TOO_MANY_REQUESTS;
            case 503:
                return COSErrorType.SERVICE_UNAVAILABLE;
            case 504:
                return COSErrorType.GATEWAY_TIMEOUT;
            default:
                return COSErrorType.INTERNAL_SERVER_ERROR;
        }
    }

    private COSErrorType mapClientError(CosClientException e) {
        String message = e.getMessage().toLowerCase();
        if (message.contains("network") || message.contains("connection")) {
            return COSErrorType.SERVICE_UNAVAILABLE;
        } else if (message.contains("credentials") || message.contains("authentication")) {
            return COSErrorType.UNAUTHORIZED;
        }
        return COSErrorType.INTERNAL_SERVER_ERROR;
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
