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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Performs Tencent COS object operations (upload and delete) with retry handling.
 *
 * <p>Used by {@link com.gotocompany.firehose.sink.common.blobstorage.cos.CloudObjectStorage} to upload bytes
 * or local files and to delete objects. Service errors with retryable status codes are retried up to the
 * configured maximum with a linearly increasing backoff; failures are mapped to a {@link BlobStorageException}
 * whose error type comes from {@link COSErrorType}.
 */
public class TencentObjectOperations {
    /** Logger for upload and delete attempts and outcomes. */
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentObjectOperations.class);
    /** Path separator used to normalise object keys. */
    private static final String OBJECT_PATH_SEPARATOR = "/";

    /** HTTP 400 Bad Request status code. */
    private static final int HTTP_BAD_REQUEST = 400;
    /** HTTP 401 Unauthorized status code. */
    private static final int HTTP_UNAUTHORIZED = 401;
    /** HTTP 403 Forbidden status code. */
    private static final int HTTP_FORBIDDEN = 403;
    /** HTTP 404 Not Found status code. */
    private static final int HTTP_NOT_FOUND = 404;
    /** HTTP 405 Method Not Allowed status code. */
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    /** HTTP 409 Conflict status code. */
    private static final int HTTP_CONFLICT = 409;
    /** HTTP 429 Too Many Requests status code (retryable). */
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    /** HTTP 503 Service Unavailable status code (retryable). */
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    /** HTTP 504 Gateway Timeout status code (retryable). */
    private static final int HTTP_GATEWAY_TIMEOUT = 504;
    /** Number of upload attempts used when the configuration does not specify one. */
    private static final int DEFAULT_MAX_RETRIES = 3;
    /** HTTP 500 Internal Server Error status code; 5xx codes are treated as retryable. */
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;

    /** COS client used to perform the object operations. */
    private final COSClient cosClient;
    /** Bound COS configuration. */
    private final CloudObjectStorageConfig config;
    /** Maximum number of upload attempts before failing. */
    private final int maxRetries;
    /** Base backoff in milliseconds, multiplied by the attempt number between retries. */
    private final long retryDelayMs;

    /**
     * Creates the operations helper, validating its collaborators and resolving retry settings.
     *
     * @param cosClient the COS client used to perform operations
     * @param config    the bound COS configuration
     * @throws IllegalArgumentException if the client, configuration or bucket name is missing
     */
    public TencentObjectOperations(COSClient cosClient, CloudObjectStorageConfig config) {
        if (cosClient == null) {
            throw new IllegalArgumentException("COSClient cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("CloudObjectStorageConfig cannot be null");
        }
        String bucketName = config.getCosBucketName();
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        this.cosClient = cosClient;
        this.config = config;
        this.maxRetries = config.getCosRetryMaxAttempts() != null
                ? config.getCosRetryMaxAttempts()
                : DEFAULT_MAX_RETRIES;
        this.retryDelayMs = config.getCosRetryDelayMS();
    }

    /**
     * Strips a leading separator from the object key to form the blob path.
     *
     * @param objectKey the requested object key
     * @return the normalised blob path
     * @throws IllegalArgumentException if the object key is null or empty
     */
    private String buildObjectPath(String objectKey) {
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        return objectKey.startsWith(OBJECT_PATH_SEPARATOR) ? objectKey.substring(1) : objectKey;
    }

    /**
     * Uploads the given bytes to COS, retrying retryable service errors.
     *
     * @param objectKey the destination object key
     * @param content   the bytes to upload
     * @throws BlobStorageException     if the upload fails after exhausting retries, or is interrupted
     * @throws IllegalArgumentException if the object key is null or empty, or the content is null
     */
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
                cosClient.putObject(config.getCosBucketName(), blobPath, new ByteArrayInputStream(content), metadata);
                LOGGER.info("Successfully uploaded content to COS: {}", blobPath);
                return;
            } catch (CosServiceException e) {
                lastException = e;
                COSErrorType errorType = getErrorType(e.getStatusCode());
                if (!isRetryableError(e.getStatusCode())) {
                    LOGGER.error("Non-retryable service error while uploading to COS: {} - {} ({})",
                            blobPath, e.getErrorMessage(), e.getStatusCode());
                    throw new BlobStorageException(errorType.name(), e.getErrorMessage(), e);
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

        if (lastException instanceof CosServiceException) {
            CosServiceException e = (CosServiceException) lastException;
            throw new BlobStorageException(getErrorType(e.getStatusCode()).name(),
                    String.format("Failed to upload after %d attempts: %s", maxRetries, e.getErrorMessage()),
                    lastException);
        } else {
            throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(),
                    String.format("Failed to upload after %d attempts: %s", maxRetries, lastException.getMessage()),
                    lastException);
        }
    }

    /**
     * Uploads a local file to COS, retrying retryable service errors.
     *
     * @param objectKey the destination object key
     * @param filePath  the path of the local file to upload
     * @throws BlobStorageException     if the file is missing, is a directory, is inaccessible, or the upload fails after retries
     * @throws IllegalArgumentException if the object key or file path is null or empty
     */
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
                throw new BlobStorageException(COSErrorType.NOT_FOUND.name(),
                        "File does not exist: " + filePath,
                        new IOException("File not found"));
            }
            if (Files.isDirectory(path)) {
                throw new BlobStorageException(COSErrorType.BAD_REQUEST.name(),
                        "Path is a directory: " + filePath,
                        new IOException("Path is a directory"));
            }
        } catch (SecurityException e) {
            throw new BlobStorageException(COSErrorType.FORBIDDEN.name(),
                    "Access denied to file: " + filePath,
                    e);
        }

        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to upload file to COS: {} -> {}", filePath, blobPath);

        int attempts = 0;
        Exception lastException = null;

        while (attempts < maxRetries) {
            try {
                cosClient.putObject(config.getCosBucketName(), blobPath, path.toFile());
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

        String errorMessage = lastException instanceof CosServiceException
                ? ((CosServiceException) lastException).getErrorMessage()
                : lastException.getMessage();
        throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(),
                String.format("Failed to upload after %d attempts: %s", maxRetries, errorMessage), lastException);
    }

    /**
     * Reports whether a COS service status code should be retried.
     *
     * @param statusCode the HTTP status code from the service error
     * @return {@code true} for 429, 503, 504 or any 5xx status
     */
    private boolean isRetryableError(int statusCode) {
        return statusCode == HTTP_TOO_MANY_REQUESTS
                || statusCode == HTTP_SERVICE_UNAVAILABLE
                || statusCode == HTTP_GATEWAY_TIMEOUT
                || statusCode >= HTTP_INTERNAL_SERVER_ERROR;
    }

    /**
     * Maps a COS client exception to an error type based on its message.
     *
     * @param e the client exception to classify
     * @return the inferred error type
     */
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

    /**
     * Deletes an object from COS.
     *
     * @param objectKey the key of the object to delete
     * @throws BlobStorageException     if the deletion fails or the thread is interrupted
     * @throws IllegalArgumentException if the object key is null or empty
     */
    public void deleteObject(String objectKey) throws BlobStorageException {
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new BlobStorageException(COSErrorType.DEFAULT_ERROR.name(), "Delete interrupted",
                    new InterruptedException());
        }
        String blobPath = buildObjectPath(objectKey);
        LOGGER.info("Attempting to delete object from COS: {}", blobPath);
        try {
            cosClient.deleteObject(config.getCosBucketName(), objectKey);
            LOGGER.info("Successfully deleted object from COS: {}", blobPath);
        } catch (CosServiceException e) {
            LOGGER.error("COS service error while deleting {}: {} - {}",
                    blobPath, e.getErrorCode(), e.getErrorMessage());
            COSErrorType errorType = getErrorType(e.getStatusCode());
            throw new BlobStorageException(errorType.name(), e.getErrorMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("COS client error while deleting {}: {}", blobPath, e.getMessage());
            COSErrorType errorType = mapClientError(e);
            throw new BlobStorageException(errorType.name(), "Failed to delete from COS", e);
        }
    }

    /**
     * Maps an HTTP status code to its {@link COSErrorType}.
     *
     * @param statusCode the HTTP status code from the service error
     * @return the matching error type, defaulting to {@link COSErrorType#INTERNAL_SERVER_ERROR}
     */
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
                return COSErrorType.INTERNAL_SERVER_ERROR;
        }
    }
}
