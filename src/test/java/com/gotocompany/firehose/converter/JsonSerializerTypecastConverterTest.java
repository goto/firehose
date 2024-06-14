package com.gotocompany.firehose.converter;

import com.gotocompany.firehose.config.converter.JsonSerializerTypecastConverter;
import com.gotocompany.firehose.exception.JsonParseException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;
import java.util.function.Function;

public class JsonSerializerTypecastConverterTest {

    private final JsonSerializerTypecastConverter jsonSerializerTypecastConverter = new JsonSerializerTypecastConverter();

    @Test
    public void convert_GivenValidJsonConfig_ShouldConvertToPropertyMap() {
        String configJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"LONG\"}]";
        String expectedPropertyMapKey = "$.root.field";

        Map<String, Function<String, Object>> result = jsonSerializerTypecastConverter.convert(null, configJson);
        Function<String, Object> mapper = result.get(expectedPropertyMapKey);
        Object mapperResult = mapper.apply("4");

        Assertions.assertNotNull(mapper);
        Assertions.assertTrue(mapperResult instanceof Long);
        Assertions.assertEquals(4L, mapperResult);
    }

    @Test
    public void convert_GivenInvalidJsonFormat_ShouldThrowJsonParseException() {
        String malformedConfigJson = "[{\"jsonPath\": \"$.root.field\" \"type\": \"LONG\"";

        Assertions.assertThrows(JsonParseException.class,
                () -> jsonSerializerTypecastConverter.convert(null, malformedConfigJson));
    }
}
