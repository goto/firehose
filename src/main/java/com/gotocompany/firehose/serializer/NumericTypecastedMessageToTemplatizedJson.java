package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.SerializerConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.Map;
import java.util.function.Function;

public class NumericTypecastedMessageToTemplatizedJson implements MessageSerializer {

    private final MessageToTemplatizedJson messageToTemplatizedJson;
    private final SerializerConfig serializerConfig;

    public NumericTypecastedMessageToTemplatizedJson(MessageToTemplatizedJson messageToTemplatizedJson,
                                                     SerializerConfig serializerConfig) {
        this.messageToTemplatizedJson = messageToTemplatizedJson;
        this.serializerConfig = serializerConfig;
    }

    @Override
    public String serialize(Message message) throws DeserializerException {
        String templatizedOutput = messageToTemplatizedJson.serialize(message);
        DocumentContext documentContext = JsonPath.parse(templatizedOutput);

        for (Map.Entry<String, Function<String, Number>> entry : serializerConfig.serializerJsonNumericTypecast().entrySet()) {
            documentContext.map(entry.getKey(), (currentValue, configuration) -> entry.getValue().apply(currentValue.toString()));
        }
        return documentContext.jsonString();
    }
}
