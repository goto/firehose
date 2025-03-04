package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.SessionTokenGenerator;
import com.gotocompany.firehose.sink.common.blobstorage.cos.auth.TokenLifecycleManager;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.COSSessionCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.region.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

public class CloudObjectStorage implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudObjectStorage.class);
    private final CloudObjectStorageConfig cloudObjectStorageConfig;
    private final COSClient cosClient;
    private final SessionTokenGenerator sessionTokenGenerator;
    private final TokenLifecycleManager tokenLifecycleManager;

    public CloudObjectStorage(CloudObjectStorageConfig cloudObjectStorageConfig) {
        this.cloudObjectStorageConfig = cloudObjectStorageConfig;
        this.sessionTokenGenerator = new SessionTokenGenerator(cloudObjectStorageConfig);
        COSSessionCredentials cred = sessionTokenGenerator.getCOSCredentials();
        ClientConfig clientConfig = new ClientConfig(new Region(cloudObjectStorageConfig.getCosRegion()));
        this.cosClient = new COSClient(cred, clientConfig);
        this.tokenLifecycleManager = new TokenLifecycleManager(cloudObjectStorageConfig, cosClient, sessionTokenGenerator);
    }

    public CloudObjectStorage(CloudObjectStorageConfig cloudObjectStorageConfig,
                               COSClient cosClient,
                               SessionTokenGenerator sessionTokenGenerator,
                               TokenLifecycleManager tokenLifecycleManager) {
        this.cloudObjectStorageConfig = cloudObjectStorageConfig;
        this.cosClient = cosClient;
        this.sessionTokenGenerator = sessionTokenGenerator;
        this.tokenLifecycleManager = tokenLifecycleManager;
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        String finalPath = getAbsolutePath(objectName);
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(finalPath, content);
        } catch (Exception e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        tokenLifecycleManager.softRefreshCredential();
        String finalPath = getAbsolutePath(objectName);
        try {
            String contentStr = new String(content);
            cosClient.putObject(cloudObjectStorageConfig.getCOSBucketName(), finalPath, contentStr);
            LOGGER.info("Created object in COS {}", objectName);
        } catch (CosServiceException cse) {
            LOGGER.error("Failed to create object in COS {}, service happen exception", objectName);
            throw new BlobStorageException(cse.getErrorCode(), cse.getMessage(), cse);
        } catch (CosClientException cce) {
            LOGGER.error("Failed to create object in COS {}, client happen exception", objectName);
            throw new BlobStorageException(cce.getErrorCode(), cce.getMessage(), cce);
        }
    }

    private String getAbsolutePath(String objectName) {
        String prefix = cloudObjectStorageConfig.getCOSDirectoryPrefix();
        if (prefix != null && !prefix.endsWith("/")
                && objectName.startsWith("/")) {
            prefix += "/";
        }
        return prefix == null || prefix.isEmpty() ? objectName : Paths.get(prefix, objectName).toString();
    }
}
