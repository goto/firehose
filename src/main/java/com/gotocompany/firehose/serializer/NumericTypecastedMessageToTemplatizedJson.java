package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.SerializerConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.List;
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
            replaceSingleJsonPath(documentContext, entry.getKey(), entry.getValue());
        }
        return documentContext.jsonString();
    }

    private void replaceSingleJsonPath(DocumentContext documentContext,
                                      String jsonPathExpression,
                                      Function<String, Number> mapper) {
        Object originalValues = documentContext.read(jsonPathExpression);

        if (!(originalValues instanceof List)) {
            typecastSingleField(jsonPathExpression, originalValues.toString(), mapper, documentContext);
            return;
        }
        for (String originalValue : (List<String>) originalValues) {
            typecastSingleField(jsonPathExpression, originalValue, mapper, documentContext);
        }
    }

    private void typecastSingleField(String jsonPathExpression,
                                     String originalValue,
                                     Function<String, Number> mapper,
                                     DocumentContext documentContext) {
        String fieldName = jsonPathExpression.substring(jsonPathExpression.lastIndexOf(".") + 1);
        String[] segment = jsonPathExpression.split("\\." + fieldName + "$");
        String classifier = String.format("%s[?(@.%s == \"%s\")].%s", segment[0], fieldName, originalValue, fieldName);
        JsonPath jsonPath = JsonPath.compile(classifier);
        documentContext.set(jsonPath, mapper.apply(originalValue));
    }
}
