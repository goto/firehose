package com.gotocompany.firehose.sink.common.cos;

import com.gotocompany.firehose.config.COSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CredentialProvider;
import com.tencent.cloud.CosStsClient;
import com.tencent.cloud.Credentials;
import com.tencent.cloud.Response;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import static org.mockito.Mockito.*;

public class CredentialProviderTest {
    @Test
    public void testGetCOSCredentials() {
        COSConfig cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
            put("COS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_COS_REGION", "ap-guangzhou");
            put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
            put("SOME_TYPE_COS_DIRECTORY_PREFIX", "prefix");
            put("SOME_TYPE_COS_SECRET_ID", "secretId");
            put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
            put("SOME_TYPE_COS_APPID", "appid");
        }});
        CredentialProvider provider = new CredentialProvider(cosConfig);

        Response resp = new Response();
        try {
            Credentials c = new Credentials();
            setFieldValue(c, "tmpSecretId", "test");
            setFieldValue(c, "tmpSecretKey", "test");
            setFieldValue(c, "sessionToken", "test");
            setFieldValue(resp, "credentials", c);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail("Error setting field value: " + e.getMessage());
        }
        try (MockedStatic<CosStsClient> mockedStatic = mockStatic(CosStsClient.class)) {
            mockedStatic.when(() -> CosStsClient.getCredential(any(TreeMap.class))).thenReturn(resp);
            provider.getCOSCredentials();
            mockedStatic.verify(() -> CosStsClient.getCredential(any(TreeMap.class)), times(1));
        }
    }

    @Test
    public void testGetResource() {
        List<String[]> cases = Arrays.asList(new String[][]{
                {"", "qcs::cos:ap-guangzhou:uid/appid:bucket/*"},
                {"/", "qcs::cos:ap-guangzhou:uid/appid:bucket/*"},
                {"prefix", "qcs::cos:ap-guangzhou:uid/appid:bucket/prefix/*"},
                {"/prefix", "qcs::cos:ap-guangzhou:uid/appid:bucket/prefix/*"},
                {"prefix/", "qcs::cos:ap-guangzhou:uid/appid:bucket/prefix/*"},
                {"/prefix/", "qcs::cos:ap-guangzhou:uid/appid:bucket/prefix/*"},
        });

        for (String[] testCase : cases) {
            COSConfig cosConfig = ConfigFactory.create(COSConfig.class, new HashMap<String, Object>() {{
                put("COS_TYPE", "SOME_TYPE");
                put("SOME_TYPE_COS_REGION", "ap-guangzhou");
                put("SOME_TYPE_COS_BUCKET_NAME", "bucket");
                put("SOME_TYPE_COS_DIRECTORY_PREFIX", testCase[0]);
                put("SOME_TYPE_COS_SECRET_ID", "secretId");
                put("SOME_TYPE_COS_SECRET_KEY", "secretKey");
                put("SOME_TYPE_COS_APPID", "appid");
            }});
            CredentialProvider provider = new CredentialProvider(cosConfig);

            try {
                Method method = CredentialProvider.class.getDeclaredMethod("getResource");
                method.setAccessible(true);
                String resource = (String) method.invoke(provider);
                Assert.assertEquals(testCase[1], resource);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                Assert.fail("Error invoking getResource method: " + e.getMessage());
            }
        }
    }

    public void setFieldValue(Object obj, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }
}
