package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.converter.SerializerConfigConverter;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TypecastedJsonSerializerTest {

    private static final String DEFAULT_JSON_MESSAGE = "{\"key\": \"value\", \"long\":\"1234568129012312\",\"nested\": {\"int\": \"1234\"}, \"double\": \"12.1\"}";
    private static final String DEFAULT_PARAMETERS = "[{\"jsonPath\": \"$..int\", \"type\": \"INTEGER\"}, {\"jsonPath\": \"$..long\", \"type\": \"LONG\"}, {\"jsonPath\": \"$..double\", \"type\": \"DOUBLE\"}]";

    private TypecastedJsonSerializer typecastedJsonSerializer;

    @Mock
    private MessageSerializer messageSerializer;

    @Mock
    private HttpSinkConfig httpSinkConfig;

    private SerializerConfigConverter serializerConfigConverter = new SerializerConfigConverter();

    @Before
    public void setup() {
        messageSerializer = Mockito.mock(MessageSerializer.class);
        httpSinkConfig = Mockito.mock(HttpSinkConfig.class);
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, DEFAULT_PARAMETERS);
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(DEFAULT_JSON_MESSAGE);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);
        typecastedJsonSerializer = new TypecastedJsonSerializer(
                messageSerializer, httpSinkConfig
        );
    }

    @Test
    public void shouldCastToNumberWhenGivenMessageWithQuoteWrappedNumberAndMatchingJsonPathConfiguration() {
        String processedJsonString = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));
        DocumentContext jsonPath = JsonPath.parse(processedJsonString);
        JSONArray integerJsonArray = jsonPath.read("$..int");
        JSONArray longJsonArray = jsonPath.read("$..long");
        JSONArray doubleJsonArray = jsonPath.read("$..double");

        Assertions.assertTrue(integerJsonArray.get(0) instanceof Integer);
        Assertions.assertTrue(longJsonArray.get(0) instanceof Long);
        Assertions.assertTrue(doubleJsonArray.get(0) instanceof Double);
        Assertions.assertEquals(integerJsonArray.get(0), 1234);
        Assertions.assertEquals(longJsonArray.get(0), 1234568129012312L);
        Assertions.assertEquals(doubleJsonArray.get(0), 12.1);
    }

    @Test
    public void shouldIgnoreWhenGivenNullMessageValue() {
        String jsonWithNullMappedValue = "{\"key\": \"value\", \"long\":null}";
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(jsonWithNullMappedValue);
        String processedJsonString = typecastedJsonSerializer.serialize(buildMessage("key", jsonWithNullMappedValue));
        DocumentContext jsonPath = JsonPath.parse(processedJsonString);
        JSONArray fieldWithValue = jsonPath.read("$..key");
        JSONArray integerJsonArray = jsonPath.read("$..long");

        Assertions.assertEquals("value", fieldWithValue.get(0));
        Assertions.assertNull(integerJsonArray.get(0));
    }

    @Test
    public void shouldReturnMessageAsItIsWhenNoJsonPathConfigurationGiven() {
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(new HashMap<>());
        typecastedJsonSerializer = new TypecastedJsonSerializer(
                messageSerializer, httpSinkConfig
        );

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));

        Assertions.assertEquals(JsonPath.parse(DEFAULT_JSON_MESSAGE).jsonString(), JsonPath.parse(result).jsonString());
    }

    @Test
    public void shouldReturnMessageAsItIsWhenJsonPathConfigurationDoesNotMatch() {
        String parameters = "[{\"jsonPath\": \"$..unrecognizedPath\", \"type\": \"INTEGER\"}]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));

        Assertions.assertEquals(JsonPath.parse(DEFAULT_JSON_MESSAGE).jsonString(), JsonPath.parse(result).jsonString());
    }

    @Test
    public void shouldThrowNumberFormatExceptionWhenPayloadTypecastIsUnparseable() {
        String payload = "{\"key\": \"value\", \"long\":\"1234568129012312\",\"nested\": {\"int\": \"1234\"}, \"double\": \"12.1\"}";
        String parameters = "[{\"jsonPath\": \"$.key\", \"type\": \"INTEGER\"}]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(payload);

        Assertions.assertThrows(NumberFormatException.class,
                () -> typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE)));
    }

    @Test
    public void shouldHandleEmptyJsonMessage() {
        String emptyJsonMessage = "{}";
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(emptyJsonMessage);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", emptyJsonMessage));

        Assertions.assertEquals(JsonPath.parse(emptyJsonMessage).jsonString(), JsonPath.parse(result).jsonString());
    }

    @Test
    public void shouldHandleEmptyJsonPathConfiguration() {
        String parameters = "[]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));

        Assertions.assertEquals(JsonPath.parse(DEFAULT_JSON_MESSAGE).jsonString(), JsonPath.parse(result).jsonString());
    }

    @Test
    public void shouldHandleInvalidJsonMessage() {
        String invalidJsonMessage = "{\"key\": \"value\", \"long\":}";
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(invalidJsonMessage);

        Assertions.assertThrows(InvalidJsonException.class,
                () -> typecastedJsonSerializer.serialize(buildMessage("key", invalidJsonMessage)));
    }

    @Test
    public void shouldHandleNonMatchingJsonPathConfiguration() {
        String parameters = "[{\"jsonPath\": \"$..nonExistentField\", \"type\": \"INTEGER\"}]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));

        Assertions.assertEquals(JsonPath.parse(DEFAULT_JSON_MESSAGE).jsonString(), JsonPath.parse(result).jsonString());
    }

    @Test
    public void shouldHandleNestedJsonPathConfiguration() {
        String parameters = "[{\"jsonPath\": \"$..nested.int\", \"type\": \"INTEGER\"}]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));
        DocumentContext jsonPath = JsonPath.parse(result);
        JSONArray nestedIntJsonArray = jsonPath.read("$..nested.int");

        Assertions.assertEquals(nestedIntJsonArray.get(0), 1234);
    }

    @Test
    public void shouldHandleMultipleJsonPathConfigurations() {
        String parameters = "[{\"jsonPath\": \"$..int\", \"type\": \"INTEGER\"}, {\"jsonPath\": \"$..double\", \"type\": \"DOUBLE\"}]";
        Map<String, Function<String, Object>> property = serializerConfigConverter.convert(null, parameters);
        Mockito.when(httpSinkConfig.getJsonTypecastMapping()).thenReturn(property);

        String result = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));
        DocumentContext jsonPath = JsonPath.parse(result);
        JSONArray intJsonArray = jsonPath.read("$..int");
        JSONArray doubleJsonArray = jsonPath.read("$..double");

        Assertions.assertEquals(intJsonArray.get(0), 1234);
        Assertions.assertEquals(doubleJsonArray.get(0), 12.1);
    }


    private Message buildMessage(String key, String payload) {
        return new Message(
                key.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8),
                "topic",
                1,
                1
        );
    }
}
