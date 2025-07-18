package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.auth.COSSessionCredentials;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Response;
import com.tencent.cloud.Credentials;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.TreeMap;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TencentSecurityTokenServiceTest {
    private static final String SECRET_ID = "test-secret-id";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String REGION = "test-region";
    private static final String APP_ID = "test-app-id";
    private static final String DIRECTORY_PREFIX = "/test/prefix";
    private static final Integer VALIDITY_SECONDS = 1800;
    private static final String TMP_SECRET_ID = "tmp-secret-id";
    private static final String TMP_SECRET_KEY = "tmp-secret-key";
    private static final String SESSION_TOKEN = "session-token";

    private TencentSecurityTokenService securityTokenService;
    @Mock
    private CloudObjectStorageConfig config;

    @Before
    public void setUp() {
        when(config.getCosSecretId()).thenReturn(SECRET_ID);
        when(config.getCosSecretKey()).thenReturn(SECRET_KEY);
        when(config.getCosBucketName()).thenReturn(BUCKET);
        when(config.getCosRegion()).thenReturn(REGION);
        when(config.getCosAppId()).thenReturn(APP_ID);
        when(config.getCosDirectoryPrefix()).thenReturn(DIRECTORY_PREFIX);
        when(config.getCosTempCredentialValiditySeconds()).thenReturn(VALIDITY_SECONDS);

        securityTokenService = new TencentSecurityTokenService(config);
    }

    @Test
    public void shouldGenerateTemporaryCredentialsSuccessfully() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = TMP_SECRET_ID;
            mockResponse.credentials.tmpSecretKey = TMP_SECRET_KEY;
            mockResponse.credentials.sessionToken = SESSION_TOKEN;
            mockResponse.expiredTime = System.currentTimeMillis() / 1000 + VALIDITY_SECONDS;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            COSSessionCredentials credentials = securityTokenService.generateTemporaryCredentials();

            assertNotNull(credentials);
            assertEquals(TMP_SECRET_ID, credentials.getCOSAccessKeyId());
            assertEquals(TMP_SECRET_KEY, credentials.getCOSSecretKey());
            assertEquals(SESSION_TOKEN, credentials.getSessionToken());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHandleNullResponse() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(null);

            securityTokenService.generateTemporaryCredentials();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHandleNullCredentials() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = null;
            mockResponse.expiredTime = System.currentTimeMillis() / 1000 + VALIDITY_SECONDS;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            securityTokenService.generateTemporaryCredentials();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHandleEmptyCredentials() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = "";
            mockResponse.credentials.tmpSecretKey = "";
            mockResponse.credentials.sessionToken = "";
            mockResponse.expiredTime = System.currentTimeMillis() / 1000 + VALIDITY_SECONDS;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            securityTokenService.generateTemporaryCredentials();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHandleNullConfigValues() throws Exception {
        when(config.getCosSecretId()).thenReturn(null);
        when(config.getCosSecretKey()).thenReturn(null);
        when(config.getCosBucketName()).thenReturn(null);
        when(config.getCosRegion()).thenReturn(null);

        securityTokenService.generateTemporaryCredentials();
    }

    @Test
    public void shouldHandleEmptyDirectoryPrefix() throws Exception {
        when(config.getCosDirectoryPrefix()).thenReturn("");

        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = TMP_SECRET_ID;
            mockResponse.credentials.tmpSecretKey = TMP_SECRET_KEY;
            mockResponse.credentials.sessionToken = SESSION_TOKEN;
            mockResponse.expiredTime = System.currentTimeMillis() / 1000 + VALIDITY_SECONDS;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            COSSessionCredentials credentials = securityTokenService.generateTemporaryCredentials();
            assertNotNull(credentials);
        }
    }

    @Test
    public void shouldHandleNullDirectoryPrefix() throws Exception {
        when(config.getCosDirectoryPrefix()).thenReturn(null);

        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            Response mockResponse = new Response();
            mockResponse.credentials = new Credentials();
            mockResponse.credentials.tmpSecretId = TMP_SECRET_ID;
            mockResponse.credentials.tmpSecretKey = TMP_SECRET_KEY;
            mockResponse.credentials.sessionToken = SESSION_TOKEN;
            mockResponse.expiredTime = System.currentTimeMillis() / 1000 + VALIDITY_SECONDS;

            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenReturn(mockResponse);

            COSSessionCredentials credentials = securityTokenService.generateTemporaryCredentials();
            assertNotNull(credentials);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHandleExceptionFromStsClient() throws Exception {
        try (MockedStatic<CosStsClient> mockedStatic = Mockito.mockStatic(CosStsClient.class)) {
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class)))
                    .thenThrow(new RuntimeException("STS client error"));

            securityTokenService.generateTemporaryCredentials();
        }
    }
}
