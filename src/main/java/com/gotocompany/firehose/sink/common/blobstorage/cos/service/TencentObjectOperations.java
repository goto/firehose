package com.gotocompany.firehose.sink.common.blobstorage.cos.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.error.COSErrorType;
import com.qcloud.cos.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;

public class TencentObjectOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentObjectOperations.class);
    private static final String OBJECT_PATH_SEPARATOR = "/";

    // HTTP Status Codes
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP_GATEWAY_TIMEOUT = 504;

    private final COSClient cosClient;
    private final CloudObjectStorageConfig config;

    public TencentObjectOperations(COSClient cosClient, CloudObjectStorageConfig config) {
        this.cosClient = cosClient;
        this.config = config;
    }

    public void uploadObject(String objectKey, byte[] content) throws BlobStorageException {
        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to upload content to COS: {}", blobPath);
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            cosClient.putObject(config.getCosBucketName(), objectKey, new ByteArrayInputStream(content), metadata);
            LOGGER.info("Successfully uploaded content to COS: {}", blobPath);
        } catch (CosServiceException e) {
            LOGGER.error("Failed to upload content to COS: {} - {} ({})",
                blobPath, e.getErrorMessage(), e.getStatusCode(), e);
            throw new BlobStorageException(getErrorType(e.getStatusCode()).name(), e.getErrorMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("Client error while uploading content to COS: {}", blobPath, e);
            throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(), "Failed to upload content to COS due to client error", e);
        }
    }

    public void uploadObject(String objectKey, String filePath) throws BlobStorageException {
        String blobPath = String.join("/", config.getCosBucketName(), objectKey);
        LOGGER.info("Attempting to upload file to COS: {} -> {}", filePath, blobPath);
        try {
            cosClient.putObject(config.getCosBucketName(), objectKey, Paths.get(filePath).toFile());
            LOGGER.info("Successfully uploaded file to COS: {}", blobPath);
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
            case HTTP_BAD_REQUEST:
                return COSErrorType.BAD_REQUEST;
            case HTTP_UNAUTHORIZED:
                return COSErrorType.UNAUTHORIZED;
            case HTTP_FORBIDDEN:
                return COSErrorType.FORBIDDEN;
            case HTTP_NOT_FOUND:
                return COSErrorType.NOT_FOUND;
            case HTTP_METHOD_NOT_ALLOWED:
                return COSErrorType.METHOD_NOT_ALLOWED;
            case HTTP_CONFLICT:
                return COSErrorType.CONFLICT;
            case HTTP_TOO_MANY_REQUESTS:
                return COSErrorType.TOO_MANY_REQUESTS;
            case HTTP_SERVICE_UNAVAILABLE:
                return COSErrorType.SERVICE_UNAVAILABLE;
            case HTTP_GATEWAY_TIMEOUT:
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

    private COSErrorType mapHttpStatusToErrorType(int statusCode) {
        switch (statusCode) {
            case HTTP_BAD_REQUEST:
                return COSErrorType.BAD_REQUEST;
            case HTTP_UNAUTHORIZED:
                return COSErrorType.UNAUTHORIZED;
            case HTTP_FORBIDDEN:
                return COSErrorType.FORBIDDEN;
            case HTTP_NOT_FOUND:
                return COSErrorType.NOT_FOUND;
            case HTTP_METHOD_NOT_ALLOWED:
                return COSErrorType.METHOD_NOT_ALLOWED;
            case HTTP_CONFLICT:
                return COSErrorType.CONFLICT;
            case HTTP_TOO_MANY_REQUESTS:
                return COSErrorType.TOO_MANY_REQUESTS;
            case HTTP_SERVICE_UNAVAILABLE:
                return COSErrorType.SERVICE_UNAVAILABLE;
            case HTTP_GATEWAY_TIMEOUT:
                return COSErrorType.GATEWAY_TIMEOUT;
            default:
                return COSErrorType.DEFAULT_ERROR;
        }
    }

    public String buildObjectPath(String objectKey) {
        return String.join(OBJECT_PATH_SEPARATOR, config.getCosBucketName(), objectKey);
    }

    public void deleteObject(String objectKey) throws BlobStorageException {
        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to delete object from COS: {}", blobPath);
        try {
            cosClient.deleteObject(config.getCosBucketName(), objectKey);
            LOGGER.info("Successfully deleted object from COS: {}", blobPath);
        } catch (CosServiceException e) {
            LOGGER.error("Failed to delete object from COS: {} - {} ({})",
                blobPath, e.getErrorMessage(), e.getStatusCode(), e);
            throw new BlobStorageException(getErrorType(e.getStatusCode()).name(), e.getErrorMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("Client error while deleting object from COS: {}", blobPath, e);
            throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(), "Failed to delete object from COS due to client error", e);
        }
    }

    public byte[] downloadObject(String objectKey) {
        // Implementation
        return new byte[0];
    }

    private COSErrorType getErrorType(int statusCode) {
        switch (statusCode) {
            case HTTP_BAD_REQUEST:
                return COSErrorType.BAD_REQUEST;
            case HTTP_UNAUTHORIZED:
                return COSErrorType.UNAUTHORIZED;
            case HTTP_FORBIDDEN:
                return COSErrorType.FORBIDDEN;
            case HTTP_NOT_FOUND:
                return COSErrorType.NOT_FOUND;
            case HTTP_METHOD_NOT_ALLOWED:
                return COSErrorType.METHOD_NOT_ALLOWED;
            case HTTP_CONFLICT:
                return COSErrorType.CONFLICT;
            case HTTP_TOO_MANY_REQUESTS:
                return COSErrorType.TOO_MANY_REQUESTS;
            case HTTP_SERVICE_UNAVAILABLE:
                return COSErrorType.SERVICE_UNAVAILABLE;
            case HTTP_GATEWAY_TIMEOUT:
                return COSErrorType.GATEWAY_TIMEOUT;
            default:
                return COSErrorType.DEFAULT_ERROR;
        }
    }
}
