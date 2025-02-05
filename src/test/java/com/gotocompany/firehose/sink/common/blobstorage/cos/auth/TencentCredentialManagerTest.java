package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.tencent.cloud.Response;
import com.tencent.cloud.Credentials;
import com.tencent.cloud.CosStsClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.TreeMap;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TencentCredentialManagerTest {
    private static final Integer VALIDITY_SECONDS = 1800;
    private static final String SECRET_ID = "test-secret-id";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String REGION = "test-region";
    private static final String ALLOW_PREFIX = "*";
    private static final String TMP_SECRET_ID = "tmp-secret-id";
    private static final String TMP_SECRET_KEY = "tmp-secret-key";
    private static final String SESSION_TOKEN = "session-token";

    private TencentCredentialManager credentialManager;
    @Mock
    private CloudObjectStorageConfig config;

    @Before
    public void setUp() {
        when(config.getCosSecretId()).thenReturn(SECRET_ID);
        when(config.getCosSecretKey()).thenReturn(SECRET_KEY);
        when(config.getCosBucketName()).thenReturn(BUCKET);
        when(config.getCosRegion()).thenReturn(REGION);
        when(config.getCosTempCredentialValiditySeconds()).thenReturn(VALIDITY_SECONDS);

        credentialManager = new TencentCredentialManager(config);
    }

    @Test
    public void shouldRefreshCredentialsSuccessfully() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = TMP_SECRET_ID;
            mockResponse.credentials.tmpSecretKey = TMP_SECRET_KEY;
            mockResponse.credentials.sessionToken = SESSION_TOKEN;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            credentialManager.refresh();

            assertEquals(TMP_SECRET_ID, credentialManager.getCredentials().getCOSAccessKeyId());
            assertEquals(TMP_SECRET_KEY, credentialManager.getCredentials().getCOSSecretKey());
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullResponse() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(null);

            credentialManager.refresh();
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullCredentials() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = null;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            credentialManager.refresh();
        }
    }

    @Test
    public void shouldHandleCredentialExpiration() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = TMP_SECRET_ID;
            mockResponse.credentials.tmpSecretKey = TMP_SECRET_KEY;
            mockResponse.credentials.sessionToken = SESSION_TOKEN;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            credentialManager.refresh();

            Field lastUpdateTimeField = TencentCredentialManager.class.getDeclaredField("lastUpdateTime");
            lastUpdateTimeField.setAccessible(true);
            lastUpdateTimeField.set(credentialManager, System.currentTimeMillis() - (VALIDITY_SECONDS * 1000 + 1000));

            credentialManager.getCredentials();
            assertNotNull(credentialManager.getCredentials());
        }
    }
} 