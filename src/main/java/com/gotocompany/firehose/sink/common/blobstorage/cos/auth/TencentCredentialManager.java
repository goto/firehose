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

/**
 * COS credentials provider that issues and caches temporary STS credentials.
 *
 * <p>Implements {@link COSCredentialsProvider} so it can be supplied directly to a
 * {@link com.qcloud.cos.COSClient}. Credentials are fetched from the Tencent STS endpoint using the secrets
 * in {@link CloudObjectStorageConfig} and refreshed lazily once their configured validity has elapsed.
 */
public class TencentCredentialManager implements COSCredentialsProvider {
    /** Logger for credential refresh outcomes. */
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentCredentialManager.class);
    /** Divisor (milliseconds per second) used to convert elapsed time when checking expiry. */
    private static final int CREDENTIAL_REFRESH_THRESHOLD_MS = 1000;

    /** Bound COS configuration supplying secrets, bucket, region and validity. */
    private final CloudObjectStorageConfig config;
    /** Currently cached credentials, or {@code null} before the first refresh. */
    private COSCredentials credentials;
    /** Epoch time, in milliseconds, when the cached credentials were last refreshed. */
    private long lastUpdateTime;

    /**
     * Creates a credential manager bound to the COS configuration with no credentials cached yet.
     *
     * @param config the bound COS configuration
     */
    public TencentCredentialManager(CloudObjectStorageConfig config) {
        this.config = config;
        this.credentials = null;
        this.lastUpdateTime = 0;
    }

    /**
     * Returns valid COS credentials, refreshing them first if they are missing or expired.
     *
     * @return the current COS credentials
     */
    @Override
    public COSCredentials getCredentials() {
        if (shouldRefreshCredentials()) {
            refreshCredentials();
        }
        return credentials;
    }

    /**
     * Forces an immediate refresh of the cached credentials.
     */
    @Override
    public void refresh() {
        refreshCredentials();
    }

    /**
     * Reports whether the credentials need to be refreshed.
     *
     * @return {@code true} when no credentials are cached or they have expired
     */
    private boolean shouldRefreshCredentials() {
        return credentials == null || isCredentialsExpired();
    }

    /**
     * Reports whether the cached credentials have outlived their configured validity.
     *
     * @return {@code true} when the elapsed time since the last refresh exceeds the configured validity
     */
    private boolean isCredentialsExpired() {
        return (System.currentTimeMillis() - lastUpdateTime) / CREDENTIAL_REFRESH_THRESHOLD_MS >= config.getCosTempCredentialValiditySeconds();
    }

    /**
     * Fetches fresh temporary credentials from the STS endpoint and updates the cache.
     *
     * @throws RuntimeException if the STS call fails or returns no credentials
     */
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
