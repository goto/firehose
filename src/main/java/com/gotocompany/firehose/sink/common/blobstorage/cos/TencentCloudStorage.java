package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.COSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
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

public class TencentCloudStorage implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentCloudStorage.class);
    private final COSConfig cosConfig;
    private final COSClient cosClient;
    private final CredentialProvider credentialProvider;
    private final CredentialRefresher credentialRefresher;

    public TencentCloudStorage(COSConfig cosConfig) {
        this.cosConfig = cosConfig;
        this.credentialProvider = new CredentialProvider(cosConfig);
        COSSessionCredentials cred = credentialProvider.getCOSCredentials();
        ClientConfig clientConfig = new ClientConfig(new Region(cosConfig.getCosRegion()));
        this.cosClient = new COSClient(cred, clientConfig);
        this.credentialRefresher = new CredentialRefresher(cosConfig, cosClient, credentialProvider);
    }

    public TencentCloudStorage(COSConfig cosConfig,
                               COSClient cosClient,
                               CredentialProvider credentialProvider,
                               CredentialRefresher credentialRefresher) {
        this.cosConfig = cosConfig;
        this.cosClient = cosClient;
        this.credentialProvider = credentialProvider;
        this.credentialRefresher = credentialRefresher;
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
        credentialRefresher.softRefreshCredential();
        String finalPath = getAbsolutePath(objectName);
        try {
            String contentStr = new String(content);
            cosClient.putObject(cosConfig.getCOSBucketName(), finalPath, contentStr);
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
        String prefix = cosConfig.getCOSDirectoryPrefix();
        if (prefix != null && !prefix.endsWith("/")
                && objectName.startsWith("/")) {
            prefix += "/";
        }
        return prefix == null || prefix.isEmpty() ? objectName : Paths.get(prefix, objectName).toString();
    }
}
