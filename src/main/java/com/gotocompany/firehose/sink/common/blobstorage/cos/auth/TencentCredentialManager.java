package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.qcloud.cos.auth.COSSessionCredentials;
import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TencentCredentialManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentCredentialManager.class);
    private final TencentSecurityTokenService securityTokenService;
    private volatile long lastUpdateTime;
    private volatile COSSessionCredentials currentCredentials;
    private final CloudObjectStorageConfig config;

    public TencentCredentialManager(CloudObjectStorageConfig config) {
        this.config = config;
        this.securityTokenService = new TencentSecurityTokenService(config);
        this.lastUpdateTime = 0;
    }

    public synchronized COSSessionCredentials getCurrentCredentials() {
        LOGGER.debug("Checking if credentials need refresh");
        try {
            if (shouldRefreshCredentials()) {
                LOGGER.info("Refreshing COS credentials");
                refreshCredentials();
            }
            return currentCredentials;
        } catch (Exception e) {
            LOGGER.error("Failed to get current credentials", e);
            throw new IllegalStateException("Unable to obtain valid credentials", e);
        }
    }

    private boolean shouldRefreshCredentials() {
        return currentCredentials == null ||
                (System.currentTimeMillis() - lastUpdateTime) / 1000 >=
                        config.getCosTempCredentialValiditySeconds();
    }

    private void refreshCredentials() {
        try {
            LOGGER.debug("Requesting new temporary credentials");
            currentCredentials = securityTokenService.generateTemporaryCredentials();
            lastUpdateTime = System.currentTimeMillis();
            LOGGER.info("Successfully refreshed COS credentials, valid for {} seconds", 
                config.getCosTempCredentialValiditySeconds());
        } catch (Exception e) {
            LOGGER.error("Failed to refresh Tencent COS credentials: {}", e.getMessage(), e);
            throw new IllegalStateException("Unable to refresh credentials: " + e.getMessage(), e);
        }
    }
}
