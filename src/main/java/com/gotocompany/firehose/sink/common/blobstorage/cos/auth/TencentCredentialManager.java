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
        if (shouldRefreshCredentials()) {
            refreshCredentials();
        }
        return currentCredentials;
    }

    private boolean shouldRefreshCredentials() {
        return currentCredentials == null ||
                (System.currentTimeMillis() - lastUpdateTime) / 1000 >=
                        config.getCosTempCredentialValiditySeconds();
    }

    private void refreshCredentials() {
        try {
            currentCredentials = securityTokenService.generateTemporaryCredentials();
            lastUpdateTime = System.currentTimeMillis();
        } catch (Exception e) {
            LOGGER.error("Failed to refresh Tencent COS credentials", e);
            throw new IllegalStateException("Unable to refresh credentials", e);
        }
    }
}
