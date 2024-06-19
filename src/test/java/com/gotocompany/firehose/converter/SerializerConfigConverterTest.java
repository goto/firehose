package com.gotocompany.firehose.converter;

import com.gotocompany.firehose.config.converter.SerializerConfigConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;
import java.util.function.Function;

public class SerializerConfigConverterTest {

    private final SerializerConfigConverter serializerConfigConverter = new SerializerConfigConverter();

    @Test
    public void convertShouldConvertToPropertyMapWhenValidJsonConfig() {
        String configJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"LONG\"}]";
        String expectedPropertyMapKey = "$.root.field";

        Map<String, Function<String, Object>> result = serializerConfigConverter.convert(null, configJson);
        Function<String, Object> mapper = result.get(expectedPropertyMapKey);
        Object mapperResult = mapper.apply("4");

        Assertions.assertNotNull(mapper);
        Assertions.assertTrue(mapperResult instanceof Long);
        Assertions.assertEquals(4L, mapperResult);
    }

    @Test
    public void convertShouldThrowJsonParseExceptionWhenInvalidJsonFormatProvided() {
        String malformedConfigJson = "[{\"jsonPath\": \"$.root.field\" \"type\": \"LONG\"";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> serializerConfigConverter.convert(null, malformedConfigJson));
    }

    @Test
    public void convertShouldThrowJsonParseExceptionWhenUnregisteredTypecastingProvided() {
        String malformedConfigJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"BIG_INTEGER\"}]";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> serializerConfigConverter.convert(null, malformedConfigJson));
    }
}
