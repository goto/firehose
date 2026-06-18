package com.gotocompany.firehose.serializer;


import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.stencil.Parser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;

/**
 * EsbMessageToJson Serialize protobuff message content into JSON.
 */
public class MessageToJson implements MessageSerializer {
    /** Stencil parser used to decode the protobuf key and value. */
    private Parser protoParser;
    /** Gson instance configured with the message serializer, naming, and exclusion strategy. */
    private Gson gson;
    /** Whether to keep original proto field names instead of camel-case JSON names. */
    private boolean preserveFieldNames;
    /** Whether to wrap the resulting JSON object inside a single-element array. */
    private boolean wrapInsideArray;
    /** Whether protobuf timestamp fields are rewritten as simple date-time strings. */
    private boolean enableSimpleDateFormat;

    /**
     * Creates a serializer that does not wrap the output in an array.
     *
     * @param protoParser            the parser used to decode protobuf payloads
     * @param preserveFieldNames     whether to keep original proto field names
     * @param enableSimpleDateFormat whether to reformat timestamp fields as date-time strings
     */
    public MessageToJson(Parser protoParser, boolean preserveFieldNames, boolean enableSimpleDateFormat) {
        this(protoParser, preserveFieldNames, false, enableSimpleDateFormat);
    }

    /**
     * Creates a serializer with full control over array wrapping.
     *
     * @param protoParser            the parser used to decode protobuf payloads
     * @param preserveFieldNames     whether to keep original proto field names
     * @param wrappedInsideArray     whether to wrap the JSON object in a single-element array
     * @param enableSimpleDateFormat whether to reformat timestamp fields as date-time strings
     */
    public MessageToJson(Parser protoParser, boolean preserveFieldNames, boolean wrappedInsideArray, boolean enableSimpleDateFormat) {
        this.protoParser = protoParser;
        this.preserveFieldNames = preserveFieldNames;
        this.wrapInsideArray = wrappedInsideArray;
        this.enableSimpleDateFormat = enableSimpleDateFormat;
        this.gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageJsonSerializer())
                .setExclusionStrategies(createGsonExclusionStrategy())
                .setFieldNamingStrategy(field -> field.getName().replaceAll("_", "")).create();
    }

    /**
     * Serializes the message into a JSON object containing the topic, key, and decoded payload.
     *
     * @param message the message to serialize
     * @return the JSON string, optionally wrapped in a single-element array
     * @throws DeserializerException if the protobuf payload cannot be parsed
     */
    @Override
    public String serialize(Message message) throws DeserializerException {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("topic", message.getTopic());

            if (message.getLogKey() != null && message.getLogKey().length != 0) {
                DynamicMessage key = protoParser.parse(message.getLogKey());
                jsonObject.put("logKey", this.gson.toJson(convertDynamicMessageToJson(key)));
            }

            DynamicMessage msg = protoParser.parse(message.getLogMessage());
            jsonObject.put("logMessage", this.gson.toJson(convertDynamicMessageToJson(msg)));

            if (wrapInsideArray) {
                return Collections.singletonList(jsonObject.toJSONString()).toString();
            }
            return jsonObject.toJSONString();
        } catch (InvalidProtocolBufferException | ParseException e) {
            throw new DeserializerException(e.getMessage());
        }
    }

    /**
     * Converts a decoded protobuf message into a JSON-friendly object.
     *
     * <p>Renders the message with {@link JsonFormat} and, when simple date formatting is enabled,
     * rewrites any timestamp fields to date-time strings.
     *
     * @param message the decoded protobuf message
     * @return the parsed JSON representation of the message
     * @throws ParseException                 if the rendered JSON cannot be re-parsed
     * @throws InvalidProtocolBufferException if the message cannot be printed as JSON
     */
    private Object convertDynamicMessageToJson(DynamicMessage message)
            throws ParseException, InvalidProtocolBufferException {
        Map<Descriptors.FieldDescriptor, Object> allFields = new HashMap<>();
        List<String> timeStampKeys = new ArrayList<>();

        allFields = message.getAllFields();
        for (Descriptors.FieldDescriptor key : allFields.keySet()) {
            Object field = allFields.get(key);
            boolean fieldIsTimestamp = field instanceof DynamicMessage
                    && ((DynamicMessage) field).getDescriptorForType().getName().equals(Timestamp.class.getSimpleName());
            if (fieldIsTimestamp) {
                if (preserveFieldNames) {
                    timeStampKeys.add(key.getName());
                } else {
                    timeStampKeys.add(key.getJsonName());
                }
            }
        }

        JSONObject tempJsonObject = new JSONObject();
        if (preserveFieldNames) {
            tempJsonObject.put("tempKey", JsonFormat.printer().preservingProtoFieldNames().print(message));
        } else {
            tempJsonObject.put("tempKey", JsonFormat.printer().print(message));
        }

        if (enableSimpleDateFormat) {
            for (String key : timeStampKeys) {
                convertProtoBuffTimeStampToDateTime(tempJsonObject, "tempKey", key);
            }
        }

        return new JSONParser().parse(tempJsonObject.get("tempKey").toString());
    }

    /**
     * Rewrites a single timestamp field within a JSON object to a parsed {@link Date} value.
     *
     * @param jsonObject     the JSON object holding the parent field
     * @param parentField    the key of the parent object containing the timestamp
     * @param timeStampField the key of the timestamp field to rewrite
     * @return the same JSON object with the timestamp field replaced by a date value
     * @throws ParseException if the parent field cannot be parsed as JSON
     */
    private JSONObject convertProtoBuffTimeStampToDateTime(JSONObject jsonObject, String parentField,
                                                           String timeStampField) throws ParseException {
        JSONObject parentObject = (JSONObject) new JSONParser().parse(jsonObject.get(parentField).toString());
        String timestampObject = parentObject.get(timeStampField).toString();

        Date date;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").parse(timestampObject);
        } catch (java.text.ParseException e) {
            throw new RuntimeException(String.format("Not able to parse date, %s", timestampObject));
        }
        parentObject.put(timeStampField, date);
        jsonObject.put(parentField, gson.toJson(parentObject));

        return jsonObject;
    }

    /**
     * Builds a Gson exclusion strategy that keeps only proto-style fields (those ending in "_").
     *
     * @return the exclusion strategy used when configuring Gson
     */
    private ExclusionStrategy createGsonExclusionStrategy() {
        return new ExclusionStrategy() {
            @Override
            public boolean shouldSkipField(FieldAttributes fieldAttributes) {
                return !fieldAttributes.getName().endsWith("_");
            }

            @Override
            public boolean shouldSkipClass(Class<?> aClass) {
                return false;
            }
        };
    }

}
