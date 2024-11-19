package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.COSConfig;
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

public class CredentialProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(CredentialProvider.class);

    private final COSConfig cosConfig;

    public CredentialProvider(COSConfig cosConfig) {
        this.cosConfig = cosConfig;
    }

    public COSSessionCredentials getCOSCredentials() {
        try {
            TreeMap<String, Object> config = new TreeMap<>();
            config.put("secretId", cosConfig.getCOSSecretId());
            config.put("secretKey", cosConfig.getCOSSecretKey());
            config.put("durationSeconds", cosConfig.getCOSTempCredentialValiditySeconds());
            config.put("bucket", cosConfig.getCOSBucketName());
            config.put("region", cosConfig.getCosRegion());
            Statement statement = new Statement();
            // The result of the statement setting is to allow the operation
            statement.setEffect("allow");
            /*
             * The permission list of the credential.
             * For a list of permissions, see https://cloud.tencent.com/document/product/436/31923
             * The rule is {project}:{interfaceName}
             * project : cos or ci, ci is a project used to process data stored on cos.
             * interfaceName : the authorized interface name. '*' represents authorization of all interfaces.
             */
            statement.addActions(new String[]{"cos:PutObject"});
            /*
             * Specify the resource that the credential can access, the format is as follows:
             *  1.cos : qcs::cos:{region}:uid/{appid}:{bucket}/{path}
             *  2.ci  : qcs::ci:{region}:uid/{appid}:bucket/{bucket}/{path}
             *  If "*" is filled in, the user will be allowed to access all resources.
             *  Unless required by business requirements,
             *  please grant the user the appropriate access rights according to the principle of least privilege.
             */
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
        String prefix = cosConfig.getCOSDirectoryPrefix() == null
                || cosConfig.getCOSDirectoryPrefix().isEmpty()
                ? "/" : cosConfig.getCOSDirectoryPrefix();
        if (!prefix.startsWith("/")) {
            prefix = "/" + prefix;
        }
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }
        return String.format("qcs::cos:%s:uid/%s:%s%s*", cosConfig.getCosRegion(),
                cosConfig.getCOSAppId(), cosConfig.getCOSBucketName(), prefix);
    }
}
