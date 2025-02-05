package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.qcloud.cos.auth.COSSessionCredentials;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Response;
import com.tencent.cloud.Policy;
import com.tencent.cloud.Statement;
import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;

public class TencentSecurityTokenService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TencentSecurityTokenService.class);
    private final CloudObjectStorageConfig config;

    public TencentSecurityTokenService(CloudObjectStorageConfig config) {
        this.config = config;
    }

    public COSSessionCredentials generateTemporaryCredentials() {
        validateConfig();
        TreeMap<String, Object> params = buildSecurityTokenParameters();
        Policy accessPolicy = createAccessPolicy();
        params.put("policy", accessPolicy.toString());
        try {
            Response stsResponse = CosStsClient.getCredential(params);
            validateResponse(stsResponse);
            return new BasicSessionCredentials(
                    stsResponse.credentials.tmpSecretId,
                    stsResponse.credentials.tmpSecretKey,
                    stsResponse.credentials.sessionToken
            );
        } catch (Exception e) {
            LOGGER.error("Failed to generate temporary credentials", e);
            throw new IllegalStateException("Failed to generate temporary credentials", e);
        }
    }

    private void validateConfig() {
        if (config.getCosSecretId() == null || config.getCosSecretId().trim().isEmpty()) {
            throw new IllegalStateException("COS Secret ID cannot be null or empty");
        }
        if (config.getCosSecretKey() == null || config.getCosSecretKey().trim().isEmpty()) {
            throw new IllegalStateException("COS Secret Key cannot be null or empty");
        }
        if (config.getCosBucketName() == null || config.getCosBucketName().trim().isEmpty()) {
            throw new IllegalStateException("COS Bucket Name cannot be null or empty");
        }
        if (config.getCosRegion() == null || config.getCosRegion().trim().isEmpty()) {
            throw new IllegalStateException("COS Region cannot be null or empty");
        }
    }

    private void validateResponse(Response response) {
        if (response == null) {
            throw new IllegalStateException("STS response cannot be null");
        }
        if (response.credentials == null) {
            throw new IllegalStateException("STS credentials cannot be null");
        }
        if (response.credentials.tmpSecretId == null || response.credentials.tmpSecretId.trim().isEmpty() ||
            response.credentials.tmpSecretKey == null || response.credentials.tmpSecretKey.trim().isEmpty() ||
            response.credentials.sessionToken == null || response.credentials.sessionToken.trim().isEmpty()) {
            throw new IllegalStateException("Invalid STS credentials: temporary credentials cannot be null or empty");
        }
        if (response.expiredTime <= 0) {
            throw new IllegalStateException("Invalid STS credentials: expiration time must be positive");
        }
    }

    private TreeMap<String, Object> buildSecurityTokenParameters() {
        TreeMap<String, Object> params = new TreeMap<>();
        params.put("secretId", config.getCosSecretId());
        params.put("secretKey", config.getCosSecretKey());
        params.put("durationSeconds", config.getCosTempCredentialValiditySeconds());
        params.put("bucket", config.getCosBucketName());
        params.put("region", config.getCosRegion());
        return params;
    }

    private Policy createAccessPolicy() {
        Policy policy = new Policy();
        Statement statement = new Statement();
        statement.setEffect("allow");
        statement.addActions(new String[]{
            "cos:PutObject",
            "cos:GetObject",
            "cos:HeadObject",
            "cos:DeleteObject",
            "cos:ListParts",
            "cos:ListObjects"
        });
        statement.addResource(buildResourceIdentifier());
        policy.addStatement(statement);
        return policy;
    }

    private String buildResourceIdentifier() {
        String prefix = normalizeDirectoryPrefix();
        return String.format("qcs::cos:%s:uid/%s:%s%s/*",
                config.getCosRegion(),
                config.getCosAppId(),
                config.getCosBucketName(),
                prefix);
    }

    private String normalizeDirectoryPrefix() {
        String prefix = config.getCosDirectoryPrefix();
        if (prefix == null || prefix.isEmpty()) {
            return "/";
        }
        prefix = prefix.startsWith("/") ? prefix : "/" + prefix;
        return prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    }
}
