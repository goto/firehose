package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.TencentCredentialManager;
import com.gotocompany.firehose.sink.common.blobstorage.cos.service.TencentObjectOperations;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.BucketReplicationConfiguration;
import com.qcloud.cos.model.ReplicationRule;
import com.qcloud.cos.region.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BlobStorage} implementation backed by Tencent Cloud Object Storage (COS).
 *
 * <p>Wraps a {@link COSClient} authenticated through a {@link TencentCredentialManager} and configured from
 * {@link CloudObjectStorageConfig} (region, retries and timeouts). On construction it verifies the bucket
 * exists and logs any replication/retention rules. Upload operations are delegated to
 * {@link TencentObjectOperations}, which performs retries and maps failures to {@link BlobStorageException}.
 */
public class CloudObjectStorage implements BlobStorage {
    /** Logger for bucket checks, retention policy and store operations. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudObjectStorage.class);

    /** Performs the actual upload operations with retry handling. */
    private final TencentObjectOperations tencentObjectOperations;
    /** Supplies and refreshes the COS credentials. */
    private final TencentCredentialManager credentialManager;
    /** Low-level COS client used for bucket checks and uploads. */
    private final COSClient cosClient;
    /** Bound COS configuration. */
    private final CloudObjectStorageConfig config;

    /**
     * Creates a COS storage client from configuration, verifying the bucket and logging retention rules.
     *
     * @param config the bound COS configuration
     * @throws IllegalArgumentException if the bucket or region is invalid or the bucket cannot be verified
     */
    public CloudObjectStorage(CloudObjectStorageConfig config) {
        this.config = config;
        this.credentialManager = new TencentCredentialManager(config);
        ClientConfig clientConfig = createDefaultClientConfig(config);
        this.cosClient = new COSClient(credentialManager.getCredentials(), clientConfig);
        this.tencentObjectOperations = new TencentObjectOperations(cosClient, config);
        checkBucket();
        logRetentionPolicy();
    }

    /**
     * Creates a COS storage client with supplied collaborators, primarily for testing.
     *
     * @param config            the bound COS configuration
     * @param credentialManager the credential provider to use
     * @param cosClient         the COS client to use
     * @throws IllegalArgumentException if the bucket cannot be verified
     */
    CloudObjectStorage(CloudObjectStorageConfig config, TencentCredentialManager credentialManager, COSClient cosClient) {
        this.config = config;
        this.credentialManager = credentialManager;
        this.cosClient = cosClient;
        this.tencentObjectOperations = new TencentObjectOperations(cosClient, config);
        checkBucket();
        logRetentionPolicy();
    }

    /**
     * Builds the COS client configuration (region, retries and timeouts) from the sink configuration.
     *
     * @param config the bound COS configuration
     * @return the COS client configuration
     */
    private static ClientConfig createDefaultClientConfig(CloudObjectStorageConfig config) {
        ClientConfig clientConfig = new ClientConfig(new Region(config.getCosRegion()));
        clientConfig.setMaxErrorRetry(config.getCosRetryMaxAttempts());
        clientConfig.setConnectionTimeout(config.getCosConnectionTimeoutMS().intValue());
        clientConfig.setSocketTimeout(config.getCosSocketTimeoutMS().intValue());
        return clientConfig;
    }

    /**
     * Verifies that the configured bucket and region are set and that the bucket exists.
     *
     * @throws IllegalArgumentException if the bucket name or region is missing, or the bucket cannot be verified
     */
    void checkBucket() {
        String bucketName = config.getCosBucketName();
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        String region = config.getCosRegion();
        if (region == null || region.trim().isEmpty()) {
            throw new IllegalArgumentException("Region cannot be null or empty");
        }
        try {
            if (!cosClient.doesBucketExist(bucketName)) {
                LOGGER.error("Bucket does not exist: {}", bucketName);
                LOGGER.error("Please create COS bucket before running firehose: {}", bucketName);
                throw new IllegalArgumentException("COS Bucket not found: " + bucketName);
            }
            LOGGER.info("Successfully verified COS bucket exists: {}", bucketName);
        } catch (CosServiceException e) {
            LOGGER.error("Failed to check bucket existence: {} - {} ({})",
                bucketName, e.getErrorMessage(), e.getStatusCode(), e);
            throw new IllegalArgumentException("Failed to verify COS bucket: " + e.getMessage(), e);
        } catch (CosClientException e) {
            LOGGER.error("Client error while checking bucket: {}", bucketName, e);
            throw new IllegalArgumentException("Failed to verify COS bucket due to client error", e);
        }
    }

    /**
     * Logs the bucket's replication rules, when any are configured.
     */
    private void logRetentionPolicy() {
        String bucketName = config.getCosBucketName();
        try {
            BucketReplicationConfiguration replication = cosClient.getBucketReplicationConfiguration(bucketName);
            if (replication != null && replication.getRules() != null) {
                LOGGER.info("Retention Policy for bucket: {}", bucketName);
                for (ReplicationRule rule : replication.getRules()) {
                    LOGGER.info("Rule ID: {}, Status: {}", rule.getID(), rule.getStatus());
                }
            } else {
                LOGGER.info("No retention policy configured for bucket: {}", bucketName);
            }
        } catch (CosServiceException e) {
            LOGGER.warn("Unable to fetch retention policy for bucket {}: {} ({})",
                bucketName, e.getErrorMessage(), e.getStatusCode());
        } catch (CosClientException e) {
            LOGGER.warn("Client error while fetching retention policy for bucket {}: {}",
                bucketName, e.getMessage());
        }
    }

    /**
     * Uploads a local file under the given object name.
     *
     * @param objectName the destination object name within the bucket
     * @param filePath   the path of the local file to upload
     * @throws BlobStorageException if the upload fails
     */
    public void store(String objectName, String filePath) throws BlobStorageException {
        LOGGER.info("Storing file to COS: {} -> {}", filePath, objectName);
        tencentObjectOperations.uploadObject(objectName, filePath);
    }

    /**
     * Uploads the given bytes under the given object name.
     *
     * @param objectName the destination object name within the bucket
     * @param content    the bytes to upload
     * @throws BlobStorageException     if the upload fails
     * @throws IllegalArgumentException if the content is {@code null}
     */
    public void store(String objectName, byte[] content) throws BlobStorageException {
        if (content == null) {
            throw new IllegalArgumentException("Content cannot be null");
        }
        LOGGER.info("Storing content to COS: {} ({} bytes)", objectName, content.length);
        tencentObjectOperations.uploadObject(objectName, content);
    }
}
