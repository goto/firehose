package com.gotocompany.firehose.config.converter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GrpcMetadataConverterTest {

    @Test
    public void shouldConvertConfigToMetadata() {
        Map<String, String> actualMetadata = new GrpcMetadataConverter().convert(null, ",k1:v1,k2:v2, k3:v3, $Generic.Field: $Generic.Value");
        assertEquals("v1", actualMetadata.get("k1"));
        assertEquals("v2", actualMetadata.get("k2"));
        assertEquals("v3", actualMetadata.get("k3"));
        assertEquals("$Generic.Value", actualMetadata.get("$Generic.Field"));
    }

    @Test
    public void shouldConvertToEmptyMapGivenEmptyStringConfig() {
        Map<String, String> result = new GrpcMetadataConverter().convert(null, "");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIncompleteMetadataValue() {
        new GrpcMetadataConverter().convert(null, "k1:v1,k2:v2,k3:v3,k4");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIncompleteMetadataKey() {
        new GrpcMetadataConverter().convert(null, "k1:v1,:v2,k3:v3");
    }

    @Test
    public void shouldKeepCelFilterMacroValueWhole() {
        Map<String, String> actualMetadata = new GrpcMetadataConverter().convert(null,
                "user-id:$(user.emails.filter(e, e.primary)[0].address),source:app");
        assertEquals(2, actualMetadata.size());
        assertEquals("$(user.emails.filter(e, e.primary)[0].address)", actualMetadata.get("user-id"));
        assertEquals("app", actualMetadata.get("source"));
    }

    @Test
    public void shouldNotTruncateCelTernaryValueFollowedByStaticEntry() {
        Map<String, String> actualMetadata = new GrpcMetadataConverter().convert(null,
                "k:$(user.emails.size() > 0 ? user.emails[0].address : \"\"),source:app");
        assertEquals(2, actualMetadata.size());
        assertEquals("$(user.emails.size() > 0 ? user.emails[0].address : \"\")", actualMetadata.get("k"));
        assertEquals("app", actualMetadata.get("source"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnUnclosedBracketInCelValue() {
        new GrpcMetadataConverter().convert(null, "k:$(user.emails.size(,source:app");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnUnclosedQuoteInCelValue() {
        new GrpcMetadataConverter().convert(null, "k:$(user.name == \"abc),source:app");
    }

    @Test
    public void shouldKeepExistingParsingForStaticValueWithColon() {
        Map<String, String> actualMetadata = new GrpcMetadataConverter().convert(null, "url:http://host:8080,source:app");
        assertEquals(2, actualMetadata.size());
        assertEquals("http", actualMetadata.get("url"));
        assertEquals("app", actualMetadata.get("source"));
    }


}
