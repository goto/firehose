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

public class CloudObjectStorage implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudObjectStorage.class);

    private final TencentObjectOperations tencentObjectOperations;
    private final TencentCredentialManager credentialManager;
    private final COSClient cosClient;
    private final CloudObjectStorageConfig config;

    public CloudObjectStorage(CloudObjectStorageConfig config) {
        this.config = config;
        this.credentialManager = new TencentCredentialManager(config);
        ClientConfig clientConfig = new ClientConfig(new Region(config.getCosRegion()));
        this.cosClient = new COSClient(credentialManager.getCredentials(), clientConfig);
        this.tencentObjectOperations = new TencentObjectOperations(cosClient, config);
        checkBucket();
        logRetentionPolicy();
    }

    private static ClientConfig createDefaultClientConfig(CloudObjectStorageConfig config) {
        ClientConfig clientConfig = new ClientConfig(new Region(config.getCosRegion()));
        clientConfig.setMaxErrorRetry(config.getCosRetryMaxAttempts());
        clientConfig.setConnectionTimeout(config.getCosConnectionTimeoutMS().intValue());
        clientConfig.setSocketTimeout(config.getCosSocketTimeoutMS().intValue());
        return clientConfig;
    }

    private void checkBucket() {
        String bucketName = config.getCosBucketName();
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

    public void store(String objectName, String filePath) throws BlobStorageException {
        LOGGER.info("Storing file to COS: {} -> {}", filePath, objectName);
        tencentObjectOperations.uploadObject(objectName, filePath);
    }

    public void store(String objectName, byte[] content) throws BlobStorageException {
        LOGGER.info("Storing content to COS: {} ({} bytes)", objectName, content.length);
        tencentObjectOperations.uploadObject(objectName, content);
    }
}
