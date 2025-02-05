package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;

public class TencentCredentialManager implements COSCredentialsProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentCredentialManager.class);
    private static final int CREDENTIAL_REFRESH_THRESHOLD_MS = 1000;

    private final CloudObjectStorageConfig config;
    private COSCredentials credentials;
    private long lastUpdateTime;

    public TencentCredentialManager(CloudObjectStorageConfig config) {
        this.config = config;
        this.credentials = null;
        this.lastUpdateTime = 0;
    }

    @Override
    public COSCredentials getCredentials() {
        if (shouldRefreshCredentials()) {
            refreshCredentials();
        }
        return credentials;
    }

    @Override
    public void refresh() {
        refreshCredentials();
    }

    private boolean shouldRefreshCredentials() {
        return credentials == null || isCredentialsExpired();
    }

    private boolean isCredentialsExpired() {
        return (System.currentTimeMillis() - lastUpdateTime) / 1000 >= config.getCosTempCredentialValiditySeconds();
    }

    private void refreshCredentials() {
        try {
            TreeMap<String, Object> configMap = new TreeMap<>();
            configMap.put("secretId", this.config.getCosSecretId());
            configMap.put("secretKey", this.config.getCosSecretKey());
            configMap.put("durationSeconds", this.config.getCosTempCredentialValiditySeconds());
            configMap.put("bucket", this.config.getCosBucketName());
            configMap.put("region", this.config.getCosRegion());
            configMap.put("allowPrefix", "*");
            String[] allowActions = new String[] {
                "cos:PutObject",
                "cos:DeleteObject",
                "cos:GetObject",
                "cos:HeadObject",
                "cos:ListParts",
                "cos:ListObjects"
            };
            configMap.put("allowActions", allowActions);

            Response response = CosStsClient.getCredential(configMap);
            if (response == null || response.credentials == null) {
                throw new RuntimeException("Failed to refresh COS credentials: null response or credentials");
            }
            credentials = new BasicCOSCredentials(response.credentials.tmpSecretId, response.credentials.tmpSecretKey);
            lastUpdateTime = System.currentTimeMillis();
            LOGGER.info("Successfully refreshed COS credentials");
        } catch (Exception e) {
            LOGGER.error("Failed to refresh COS credentials", e);
            throw new RuntimeException("Failed to refresh COS credentials", e);
        }
    }
}
