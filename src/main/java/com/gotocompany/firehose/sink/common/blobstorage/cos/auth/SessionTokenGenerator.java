package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSSessionCredentials;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Policy;
import com.tencent.cloud.Response;
import com.tencent.cloud.Statement;
import com.tencent.cloud.cos.util.Jackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;

public class SessionTokenGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionTokenGenerator.class);

    private final CloudObjectStorageConfig cloudObjectStorageConfig;

    public SessionTokenGenerator(CloudObjectStorageConfig cloudObjectStorageConfig) {
        this.cloudObjectStorageConfig = cloudObjectStorageConfig;
    }

    public COSSessionCredentials getCOSCredentials() {
        try {
            TreeMap<String, Object> config = new TreeMap<>();
            config.put("secretId", cloudObjectStorageConfig.getCOSSecretId());
            config.put("secretKey", cloudObjectStorageConfig.getCOSSecretKey());
            config.put("durationSeconds", cloudObjectStorageConfig.getCOSTempCredentialValiditySeconds());
            config.put("bucket", cloudObjectStorageConfig.getCOSBucketName());
            config.put("region", cloudObjectStorageConfig.getCosRegion());
            Statement statement = new Statement();
            statement.setEffect("allow");
            statement.addActions(new String[]{"cos:PutObject"});
            String resource = getResource();
            statement.addResource(resource);

            Policy policy = new Policy();
            policy.addStatement(statement);
            config.put("policy", Jackson.toJsonPrettyString(policy));

            Response response = CosStsClient.getCredential(config);
            return new BasicSessionCredentials(response.credentials.tmpSecretId, response.credentials.tmpSecretKey,
                    response.credentials.sessionToken);
        } catch (Exception e) {
            LOGGER.error("get cos credential failed.", e);
            throw new IllegalArgumentException("no valid secret!");
        }
    }

    private String getResource() {
        String prefix = cloudObjectStorageConfig.getCOSDirectoryPrefix() == null
                || cloudObjectStorageConfig.getCOSDirectoryPrefix().isEmpty()
                ? "/" : cloudObjectStorageConfig.getCOSDirectoryPrefix();
        if (!prefix.startsWith("/")) {
            prefix = "/" + prefix;
        }
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }
        return String.format("qcs::cos:%s:uid/%s:%s%s*", cloudObjectStorageConfig.getCosRegion(),
                cloudObjectStorageConfig.getCOSAppId(), cloudObjectStorageConfig.getCOSBucketName(), prefix);
    }
}
