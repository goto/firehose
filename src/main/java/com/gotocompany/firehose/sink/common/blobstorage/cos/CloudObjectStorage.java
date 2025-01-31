package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.TencentCredentialManager;
import com.gotocompany.firehose.sink.common.blobstorage.cos.service.TencentObjectOperations;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.region.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

public class CloudObjectStorage implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudObjectStorage.class);

    private final TencentObjectOperations objectOperations;
    private final TencentCredentialManager credentialManager;
    private final COSClient cosClient;

    public CloudObjectStorage(CloudObjectStorageConfig config) {
        this.credentialManager = new TencentCredentialManager(config);
        ClientConfig clientConfig = new ClientConfig(new Region(config.getCosRegion()));
        this.cosClient = new COSClient(credentialManager.getCurrentCredentials(), clientConfig);
        this.objectOperations = new TencentObjectOperations(cosClient, config);
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(objectName, content);
        } catch (Exception e) {
            LOGGER.error("Failed to read file: {}", filePath);
            throw new BlobStorageException("file_read_error", "Failed to read file", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        cosClient.setCOSCredentials(credentialManager.getCurrentCredentials());
        String finalPath = objectOperations.buildObjectPath(objectName);
        objectOperations.uploadObject(finalPath, new String(content));
    }
}