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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TencentObjectOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentObjectOperations.class);
    private static final String OBJECT_PATH_SEPARATOR = "/";

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
    private final int maxRetries;
    private final long retryDelayMs;

    public TencentObjectOperations(COSClient cosClient, CloudObjectStorageConfig config) {
        if (cosClient == null) {
            throw new IllegalArgumentException("COSClient cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("CloudObjectStorageConfig cannot be null");
        }
        this.cosClient = cosClient;
        this.config = config;
        this.maxRetries = config.getCosRetryMaxAttempts() != null ? config.getCosRetryMaxAttempts() : 3;
        this.retryDelayMs = 1000;
    }

    public void uploadObject(String objectKey, byte[] content) throws BlobStorageException {
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        if (content == null) {
            throw new IllegalArgumentException("Content cannot be null");
        }

        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to upload content to COS: {}", blobPath);

        int attempts = 0;
        Exception lastException = null;

        while (attempts < maxRetries) {
            try {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(content.length);
                cosClient.putObject(config.getCosBucketName(), objectKey, new ByteArrayInputStream(content), metadata);
                LOGGER.info("Successfully uploaded content to COS: {}", blobPath);
                return;
            } catch (CosServiceException e) {
                lastException = e;
                if (!isRetryableError(e.getStatusCode())) {
                    LOGGER.error("Non-retryable service error while uploading to COS: {} - {} ({})",
                        blobPath, e.getErrorMessage(), e.getStatusCode());
                    throw new BlobStorageException(getErrorType(e.getStatusCode()).name(), e.getErrorMessage(), e);
                }
                LOGGER.warn("Retryable service error while uploading to COS (attempt {}/{}): {} - {} ({})",
                    attempts + 1, maxRetries, blobPath, e.getErrorMessage(), e.getStatusCode());
            } catch (CosClientException e) {
                lastException = e;
                LOGGER.warn("Client error while uploading to COS (attempt {}/{}): {} - {}",
                    attempts + 1, maxRetries, blobPath, e.getMessage());
            }

            attempts++;
            if (attempts < maxRetries) {
                try {
                    Thread.sleep(retryDelayMs * attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(), "Upload interrupted", ie);
                }
            }
        }

        String errorMessage = lastException instanceof CosServiceException ?
            ((CosServiceException) lastException).getErrorMessage() : lastException.getMessage();
        throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(),
            String.format("Failed to upload after %d attempts: %s", maxRetries, errorMessage), lastException);
    }

    public void uploadObject(String objectKey, String filePath) throws BlobStorageException {
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        Path path = Paths.get(filePath);
        try {
            if (!Files.exists(path)) {
                throw new BlobStorageException(COSErrorType.NOT_FOUND.name(), "File does not exist: " + filePath, new IOException("File not found"));
            }
            if (Files.isDirectory(path)) {
                throw new BlobStorageException(COSErrorType.BAD_REQUEST.name(), "Path is a directory: " + filePath, new IOException("Path is a directory"));
            }
        } catch (SecurityException e) {
            throw new BlobStorageException(COSErrorType.FORBIDDEN.name(), "Access denied to file: " + filePath, e);
        }

        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to upload file to COS: {} -> {}", filePath, blobPath);

        int attempts = 0;
        Exception lastException = null;

        while (attempts < maxRetries) {
            try {
                cosClient.putObject(config.getCosBucketName(), objectKey, path.toFile());
                LOGGER.info("Successfully uploaded file to COS: {}", blobPath);
                return;
            } catch (CosServiceException e) {
                lastException = e;
                if (!isRetryableError(e.getStatusCode())) {
                    LOGGER.error("Non-retryable service error while uploading to COS: {} - {} ({})",
                        blobPath, e.getErrorMessage(), e.getStatusCode());
                    throw new BlobStorageException(getErrorType(e.getStatusCode()).name(), e.getErrorMessage(), e);
                }
                LOGGER.warn("Retryable service error while uploading to COS (attempt {}/{}): {} - {} ({})",
                    attempts + 1, maxRetries, blobPath, e.getErrorMessage(), e.getStatusCode());
            } catch (CosClientException e) {
                lastException = e;
                LOGGER.warn("Client error while uploading to COS (attempt {}/{}): {} - {}",
                    attempts + 1, maxRetries, blobPath, e.getMessage());
            }

            attempts++;
            if (attempts < maxRetries) {
                try {
                    Thread.sleep(retryDelayMs * attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(), "Upload interrupted", ie);
                }
            }
        }

        String errorMessage = lastException instanceof CosServiceException ?
            ((CosServiceException) lastException).getErrorMessage() : lastException.getMessage();
        throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(),
            String.format("Failed to upload after %d attempts: %s", maxRetries, errorMessage), lastException);
    }

    private boolean isRetryableError(int statusCode) {
        return statusCode == HTTP_TOO_MANY_REQUESTS ||
               statusCode == HTTP_SERVICE_UNAVAILABLE ||
               statusCode == HTTP_GATEWAY_TIMEOUT ||
               statusCode >= 500;
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
        if (message.contains("timeout") || message.contains("timed out")) {
            return COSErrorType.REQUEST_TIMEOUT;
        } else if (message.contains("length") || message.contains("size")) {
            return COSErrorType.LENGTH_REQUIRED;
        } else if (message.contains("precondition")) {
            return COSErrorType.PRECONDITION_FAILED;
        } else if (message.contains("too large") || message.contains("payload")) {
            return COSErrorType.PAYLOAD_TOO_LARGE;
        } else if (message.contains("range") || message.contains("satisfiable")) {
            return COSErrorType.REQUESTED_RANGE_NOT_SATISFIABLE;
        } else if (message.contains("gateway")) {
            return COSErrorType.BAD_GATEWAY;
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
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to delete object from COS: {}", blobPath);
        try {
            cosClient.deleteObject(config.getCosBucketName(), objectKey);
            LOGGER.info("Successfully deleted object from COS: {}", blobPath);
        } catch (CosServiceException e) {
            LOGGER.error("COS service error while deleting {}: {} - {}",
                blobPath, e.getErrorCode(), e.getErrorMessage());
            COSErrorType errorType = mapServiceError(e);
            throw new BlobStorageException(errorType.name(), e.getErrorMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("COS client error while deleting {}: {}", blobPath, e.getMessage());
            COSErrorType errorType = mapClientError(e);
            throw new BlobStorageException(errorType.name(), "Failed to delete from COS", e);
        }
    }

    private COSErrorType getErrorType(int statusCode) {
        return mapHttpStatusToErrorType(statusCode);
    }
}
