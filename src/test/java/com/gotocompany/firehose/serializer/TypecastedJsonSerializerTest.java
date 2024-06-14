package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.SerializerConfig;
import com.gotocompany.firehose.config.converter.JsonSerializerTypecastConverter;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

public class TypecastedJsonSerializerTest {

    private static final String DEFAULT_JSON_MESSAGE = "{\"key\": \"value\", \"long\":\"1234568129012312\",\"nested\": {\"int\": \"1234\"}}";
    private static final String DEFAULT_PARAMETERS = "[{\"jsonPath\": \"$..int\", \"type\": \"INTEGER\"}, {\"jsonPath\": \"$..long\", \"type\": \"LONG\"}]";

    private TypecastedJsonSerializer typecastedJsonSerializer;

    @Mock
    private MessageSerializer messageSerializer;

    @Mock
    private SerializerConfig serializerConfig;

    private JsonSerializerTypecastConverter jsonSerializerTypecastConverter = new JsonSerializerTypecastConverter();

    @Before
    public void setup() {
        messageSerializer = Mockito.mock(MessageSerializer.class);
        serializerConfig = Mockito.mock(SerializerConfig.class);
        Map<String, Function<String, Object>> property = jsonSerializerTypecastConverter.convert(null, DEFAULT_PARAMETERS);
        Mockito.when(messageSerializer.serialize(Mockito.any())).thenReturn(DEFAULT_JSON_MESSAGE);
        Mockito.when(serializerConfig.serializerJsonTypecast()).thenReturn(property);
        typecastedJsonSerializer = new TypecastedJsonSerializer(
                messageSerializer, serializerConfig
        );
    }

    @Test
    public void serialize_GivenMessageWithQuoteWrappedNumber_ShouldCastToNumber() throws JSONException {
        String processedJsonString = typecastedJsonSerializer.serialize(buildMessage("key", DEFAULT_JSON_MESSAGE));
        DocumentContext jsonPath = JsonPath.parse(processedJsonString);
        JSONArray integerJsonArray = jsonPath.read("$..int");
        JSONArray longJsonArray = jsonPath.read("$..long");

        Assertions.assertTrue(integerJsonArray.get(0) instanceof Integer);
        Assertions.assertTrue(longJsonArray.get(0) instanceof Long);
        Assertions.assertEquals(integerJsonArray.get(0), 1234);
        Assertions.assertEquals(longJsonArray.get(0), 1234568129012312L);
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
