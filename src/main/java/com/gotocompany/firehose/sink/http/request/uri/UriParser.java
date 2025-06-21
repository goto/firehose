package com.gotocompany.firehose.sink.http.request.uri;


import com.google.gson.JsonObject;
import com.gotocompany.firehose.config.enums.InputSchemaType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.stencil.Parser;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * URI parser for http requests.
 */
public class UriParser {
    private Parser protoParser;
    private String parserMode;
    private JSONParser jsonParser;


    public UriParser(Parser protoParser, String parserMode) {
        this.protoParser = protoParser;
        this.parserMode = parserMode;
        this.jsonParser = new JSONParser();
    }

    public UriParser(Parser protoParser, String parserMode, JSONParser jsonParser) {
        this.protoParser = protoParser;
        this.parserMode = parserMode;
        this.jsonParser = jsonParser;
    }

    public String parse(Message message, String serviceUrl) {
        return parseServiceUrl(message, serviceUrl);
    }

    private DynamicMessage parseEsbMessage(Message message) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(message));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Unable to parse Service URL", e);
        }
        return parsedMessage;
    }

    private JSONObject parseJsonMessage(Message message) {
        JSONObject jsonObject;
        try {
            jsonObject = (JSONObject) jsonParser.parse(new String(message.getLogMessage(), StandardCharsets.UTF_8));
        } catch (ParseException e) {
            throw new JsonParseException(e.getMessage(), e.getCause());
        }
        return jsonObject;
    }


    private String parseServiceUrl(Message message, String serviceUrl) {
        if (StringUtils.isEmpty(serviceUrl)) {
            throw new IllegalArgumentException("Service URL '" + serviceUrl + "' is invalid");
        }
        String[] urlStrings = serviceUrl.split(",");
        if (urlStrings.length == 0) {
            throw new InvalidConfigurationException("Empty Service URL configuration: '" + serviceUrl + "'");
        }
        urlStrings = Arrays
                .stream(urlStrings)
                .map(String::trim)
                .toArray(String[]::new);

        String urlPattern = urlStrings[0];
        String urlVariables = StringUtils.join(Arrays.copyOfRange(urlStrings, 1, urlStrings.length), ",");

        if (StringUtils.isEmpty(urlVariables)) {
            return urlPattern;
        }

        String renderedUrl;
        if (message.getInputSchemaType() == InputSchemaType.JSON) {
            JSONObject json = parseJsonMessage(message);
            renderedUrl = renderStringUrl(json, urlPattern, urlVariables);
        } else {
            // InputSchemaType.PROTOBUF
            DynamicMessage data = parseEsbMessage(message);
            renderedUrl = renderStringUrl(data, urlPattern, urlVariables);
        }

        return renderedUrl;
    }

    private String renderStringUrl(JSONObject jsonObject, String pattern, String patternVariables) {
        List<String> patternVariablesList = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariablesList
                .stream()
                .map(field -> getDataByFieldName(jsonObject, field))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    private Object getDataByFieldName(JSONObject jsonObject, String fieldName) {
        if (!jsonObject.containsKey(fieldName)) {
            throw new IllegalArgumentException("Invalid json field name: " + fieldName);
        }

        return jsonObject.get(fieldName);
    }


    private String renderStringUrl(DynamicMessage parsedMessage, String pattern, String patternVariables) {
        List<String> patternVariableFieldNumbers = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNumbers
                .stream()
                .map(fieldNumber -> getDataByFieldNumber(parsedMessage, fieldNumber))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    private Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
        int fieldNumberInt;
        try {
            fieldNumberInt = Integer.parseInt(fieldNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Proto Index");
        }
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
        }
        return parsedMessage.getField(fieldDescriptor);
    }

    private byte[] getPayload(Message message) {
        if (parserMode.equals("key")) {
            return message.getLogKey();
        } else {
            return message.getLogMessage();
        }
    }

}
