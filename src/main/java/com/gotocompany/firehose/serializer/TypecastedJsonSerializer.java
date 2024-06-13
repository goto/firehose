package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.SerializerConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.Map;
import java.util.function.Function;

public class TypecastedJsonSerializer implements MessageSerializer {

    private final MessageSerializer messageSerializer;
    private final SerializerConfig serializerConfig;

    public TypecastedJsonSerializer(MessageSerializer messageSerializer,
                                    SerializerConfig serializerConfig) {
        this.messageSerializer = messageSerializer;
        this.serializerConfig = serializerConfig;
    }

    @Override
    public String serialize(Message message) throws DeserializerException {
        String jsonString = messageSerializer.serialize(message);
        DocumentContext documentContext = JsonPath.parse(jsonString);

        for (Map.Entry<String, Function<String, Number>> entry : serializerConfig.serializerJsonTypecast()
                .entrySet()) {
            documentContext.map(entry.getKey(), (currentValue, configuration) -> entry.getValue()
                            .apply(currentValue.toString()));
        }
        return documentContext.jsonString();
    }
}
