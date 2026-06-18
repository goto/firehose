package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.NoRetryStrategy;
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

/**
 * {@link BlobStorage} implementation backed by Alibaba Cloud Object Storage Service (OSS).
 *
 * <p>Wraps an OSS {@link OSS} client configured from {@link ObjectStorageServiceConfig} (endpoint, region,
 * credentials, timeouts and optional retries). On construction it validates that the target bucket exists.
 * Objects are written under an optional directory prefix, with detailed logging and client-error
 * classification; failures are translated into {@link BlobStorageException}.
 */
@Slf4j
public class ObjectStorageService implements BlobStorage {

    /** OSS client used to perform the uploads. */
    private final OSS oss;
    /** Target OSS bucket name. */
    private final String ossBucketName;
    /** Optional directory prefix prepended to every object name. */
    private final String ossDirectoryPrefix;

    /**
     * Creates an OSS storage client from configuration and validates the bucket.
     *
     * @param objectStorageServiceConfig the bound OSS configuration
     */
    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig) {
        this(objectStorageServiceConfig, initializeOss(objectStorageServiceConfig));
    }

    /**
     * Creates an OSS storage client using a caller-supplied client, primarily for testing.
     *
     * @param objectStorageServiceConfig the bound OSS configuration
     * @param oss                        the OSS client to use
     */
    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig, OSS oss) {
        this.oss = oss;
        this.ossBucketName = objectStorageServiceConfig.getOssBucketName();
        this.ossDirectoryPrefix = objectStorageServiceConfig.getOssDirectoryPrefix();

        log.info("Initializing OSS client - endpoint: {}, bucket: {}, directoryPrefix: {}",
            objectStorageServiceConfig.getOssEndpoint(),
            ossBucketName,
            ossDirectoryPrefix);
        log.debug("OSS retry config - enabled: {}, maxAttempts: {}",
            objectStorageServiceConfig.isRetryEnabled(),
            objectStorageServiceConfig.getOssMaxRetryAttempts());

        logOssConfiguration(objectStorageServiceConfig);
        checkBucket();
    }

    /**
     * Builds an OSS client from configuration, applying timeouts and the retry strategy.
     *
     * @param objectStorageServiceConfig the bound OSS configuration
     * @return a configured OSS client
     */
    protected static OSS initializeOss(ObjectStorageServiceConfig objectStorageServiceConfig) {
        ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
        clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
        clientBuilderConfiguration.setSocketTimeout(objectStorageServiceConfig.getOssSocketTimeoutMs());
        clientBuilderConfiguration.setConnectionTimeout(objectStorageServiceConfig.getOssConnectionTimeoutMs());
        clientBuilderConfiguration.setConnectionRequestTimeout(objectStorageServiceConfig.getOssConnectionRequestTimeoutMs());
        clientBuilderConfiguration.setRequestTimeout(objectStorageServiceConfig.getOssRequestTimeoutMs());
        if (objectStorageServiceConfig.isRetryEnabled()) {
            clientBuilderConfiguration.setMaxErrorRetry(objectStorageServiceConfig.getOssMaxRetryAttempts());
        } else {
            clientBuilderConfiguration.setRetryStrategy(new NoRetryStrategy());
        }
        return OSSClientBuilder.create()
                .endpoint(objectStorageServiceConfig.getOssEndpoint())
                .region(objectStorageServiceConfig.getOssRegion())
                .credentialsProvider(new DefaultCredentialProvider(objectStorageServiceConfig.getOssAccessId(),
                        objectStorageServiceConfig.getOssAccessKey()))
                .clientConfiguration(clientBuilderConfiguration)
                .build();
    }

    /**
     * Uploads a local file under the given object name.
     *
     * @param objectName the destination object name, before applying the directory prefix
     * @param filePath   the path of the local file to upload
     * @throws BlobStorageException if the upload fails
     */
    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        File file = new File(filePath);
        long fileSize = file.exists() ? file.length() : 0;
        String builtPath = buildObjectPath(objectName);

        log.info("Starting OSS store operation - object: {}, filePath: {}, size: {} bytes", objectName, filePath, fileSize);
        log.debug("Built OSS object path: {}", builtPath);

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                ossBucketName,
                builtPath,
                file
        );
        putObject(putObjectRequest, objectName, fileSize);
    }

    /**
     * Uploads the given bytes under the given object name.
     *
     * @param objectName the destination object name, before applying the directory prefix
     * @param content    the bytes to upload
     * @throws BlobStorageException if the upload fails
     */
    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        String builtPath = buildObjectPath(objectName);

        log.debug("Starting OSS store operation - object: {}, size: {} bytes", objectName, content.length);
        log.debug("Built OSS object path: {}", builtPath);

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                ossBucketName,
                builtPath,
                new ByteArrayInputStream(content)
        );
        putObject(putObjectRequest, objectName, content.length);
    }

    /**
     * Executes the OSS put-object request and records timing, success and failure details.
     *
     * @param putObjectRequest the prepared put-object request
     * @param objectName       the original object name, used for logging
     * @param contentSize      the size of the uploaded content in bytes, used for logging
     * @throws BlobStorageException if the upload fails, classified from the underlying OSS error
     */
    private void putObject(PutObjectRequest putObjectRequest, String objectName, long contentSize) throws BlobStorageException {
        String builtPath = putObjectRequest.getKey();
        long startTime = System.currentTimeMillis();
        try {
            oss.putObject(putObjectRequest);
            long duration = System.currentTimeMillis() - startTime;
            String ossUrl = String.format("oss://%s/%s", ossBucketName, builtPath);
            log.info("Successfully uploaded to OSS - url: {}, size: {} bytes, duration: {}ms",
                ossUrl, contentSize, duration);

            if (log.isDebugEnabled()) {
                boolean exists = oss.doesObjectExist(ossBucketName, builtPath);
                log.debug("OSS object existence verification - bucket: {}, key: {}, exists: {}",
                    ossBucketName, builtPath, exists);
                if (!exists) {
                    log.warn("ALERT: Object reported as uploaded but verification failed - bucket: {}, key: {}",
                        ossBucketName, builtPath);
                }
            }
        } catch (ClientException e) {
            String failureType = classifyClientException(e);
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.error("Failed to put object to OSS (ClientException/{}) - bucket: {}, key: {}, object: {}, size: {} bytes, elapsedTime: {}ms, error: {}",
                failureType, ossBucketName, builtPath, objectName, contentSize, elapsedTime, e.getMessage(), e);
            throw new BlobStorageException("client_error", e.getMessage(), e);
        } catch (OSSException e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.error("Failed to put object to OSS (OSSException) - bucket: {}, key: {}, object: {}, size: {} bytes, errorCode: {}, errorMessage: {}, requestID: {}, hostID: {}, elapsedTime: {}ms",
                ossBucketName, builtPath, objectName, contentSize, e.getErrorCode(), e.getErrorMessage(), e.getRequestId(), e.getHostId(), elapsedTime, e);
            throw new BlobStorageException(e.getErrorCode(), e.getErrorMessage(), e);
        }
    }

    /**
     * Prepends the configured directory prefix to the object name, when set.
     *
     * @param objectName the base object name
     * @return the full object key including any directory prefix
     */
    private String buildObjectPath(String objectName) {
        return Optional.ofNullable(ossDirectoryPrefix)
                .map(prefix -> prefix + "/" + objectName)
                .orElse(objectName);
    }

    /**
     * Classifies an OSS client exception into a coarse failure category from its message.
     *
     * @param e the client exception to classify
     * @return a short label such as {@code TIMEOUT}, {@code CONNECTION_ERROR} or {@code UNKNOWN}
     */
    private String classifyClientException(ClientException e) {
        String msg = e.getMessage().toLowerCase();

        if (msg.contains("timeout") || msg.contains("timed out") || msg.contains("read timed out")) {
            return "TIMEOUT";
        }

        if (msg.contains("connection refused") || msg.contains("connect timed out") || msg.contains("connection reset")) {
            return "CONNECTION_ERROR";
        }

        if (msg.contains("socket") || msg.contains("broken pipe") || msg.contains("connection aborted")) {
            return "SOCKET_ERROR";
        }

        if (msg.contains("ssl") || msg.contains("certificate") || msg.contains("handshake")) {
            return "SSL_ERROR";
        }

        if (msg.contains("unknown host") || msg.contains("nodename nor servname provided") || msg.contains("name resolution")) {
            return "DNS_ERROR";
        }

        return "UNKNOWN";
    }

    /**
     * Validates that the configured bucket exists.
     *
     * @throws IllegalArgumentException if the bucket does not exist
     */
    private void checkBucket() {
        BucketList bucketList = oss.listBuckets(new ListBucketsRequest(ossBucketName,
                null, 1));
        if (bucketList.getBucketList().isEmpty()) {
            log.error("Bucket does not exist: {}", ossBucketName);
            log.error("Please create OSS bucket before running firehose: {}", ossBucketName);
            throw new IllegalArgumentException("Bucket does not exist");
        }
        log.info("Successfully validated OSS bucket: {}", ossBucketName);
    }

    /**
     * Logs the configured OSS timeouts and retry strategy at debug level.
     *
     * @param config the bound OSS configuration
     */
    private void logOssConfiguration(ObjectStorageServiceConfig config) {
        log.debug("OSS timeouts - socket: {}ms, connection: {}ms, connectionRequest: {}ms, request: {}ms",
            config.getOssSocketTimeoutMs(),
            config.getOssConnectionTimeoutMs(),
            config.getOssConnectionRequestTimeoutMs(),
            config.getOssRequestTimeoutMs());

        if (config.isRetryEnabled()) {
            log.debug("OSS retry strategy: ENABLED with maxRetryAttempts: {}", config.getOssMaxRetryAttempts());
        } else {
            log.debug("OSS retry strategy: DISABLED");
        }
    }

}
