package com.gotocompany.firehose.sink.common.cos;

import com.gotocompany.firehose.config.COSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CredentialProvider;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CredentialRefresher;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import java.util.HashMap;

public class CredentialRefresherTest {
    private CredentialProvider credentialProvider;
    private CredentialRefresher credentialRefresher;

    public void init(String credentialValidSeconds) {
        COSConfig cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
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
        credentialProvider = Mockito.mock(CredentialProvider.class);
        credentialRefresher = new CredentialRefresher(cosConfig, cosClient, credentialProvider);
        COSSessionCredentials mockCredentials = new BasicSessionCredentials("", "", "");
        when(credentialProvider.getCOSCredentials()).thenReturn(mockCredentials);
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
                credentialRefresher.softRefreshCredential();
                Thread.sleep(testCase[2]);
            }
            verify(credentialProvider, times(testCase[3])).getCOSCredentials();
        }
    }
}
