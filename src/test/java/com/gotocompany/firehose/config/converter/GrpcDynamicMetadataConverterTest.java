package com.gotocompany.firehose.config.converter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

public class GrpcDynamicMetadataConverterTest {

    @Test
    public void shouldConvertJsonToMap() {
        String input = "{\"$SomeClass.someField\":\"someValue\", \"$SomeClass.anotherField\":\"$SomeClass.value\"}";

        Map<String, Object> parsedTemplate = new GrpcDynamicMetadataConverter().convert(null, input);

        Assertions.assertEquals("someValue", parsedTemplate.get("$SomeClass.someField"));
        Assertions.assertEquals("$SomeClass.value", parsedTemplate.get("$SomeClass.anotherField"));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenInputIsMalformed() {

    }
}
