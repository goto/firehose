package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import java.util.HashMap;

public class TokenLifecycleManagerTest {
    private SessionTokenGenerator sessionTokenGenerator;
    private TokenLifecycleManager tokenLifecycleManager;

    public void init(String credentialValidSeconds) {
        CloudObjectStorageConfig cloudObjectStorageConfig = ConfigFactory.create(CloudObjectStorageConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_DIRECTORY_PREFIX", "prefix");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
            put("SOME_TYPE_COS_TEMP_CREDENTIAL_VALIDITY_SECONDS", credentialValidSeconds);
        }});
        COSClient cosClient = Mockito.mock(COSClient.class);
        sessionTokenGenerator = Mockito.mock(SessionTokenGenerator.class);
        tokenLifecycleManager = new TokenLifecycleManager(cloudObjectStorageConfig, cosClient, sessionTokenGenerator);
        COSSessionCredentials mockCredentials = new BasicSessionCredentials("", "", "");
        when(sessionTokenGenerator.getCOSCredentials()).thenReturn(mockCredentials);
    }

    @Test
    public void testRefreshCredential() throws InterruptedException {
        int[][] cases = new int[][]{
                {1, 1, 1100, 0},
                {1, 3, 1100, 2},
                {2, 3, 1100, 1}
        };

        for (int[] testCase : cases) {
            init(String.valueOf(testCase[0]));
            for (int i = 0; i < testCase[1]; i++) {
                tokenLifecycleManager.softRefreshCredential();
                Thread.sleep(testCase[2]);
            }
            verify(sessionTokenGenerator, times(testCase[3])).getCOSCredentials();
        }
    }
}
