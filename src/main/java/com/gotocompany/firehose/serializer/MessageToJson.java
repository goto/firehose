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

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * EsbMessageToJson Serialize protobuff message content into JSON.
 */
public class MessageToJson implements MessageSerializer {
    private Parser protoParser;
    private Gson gson;
    private boolean preserveFieldNames;
    private boolean wrapInsideArray;
    private boolean enableSimpleDateFormat;

    private DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("MMM dd, yyyy hh:mm:ss a", Locale.US);
    private DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US);

    public MessageToJson(Parser protoParser, boolean preserveFieldNames, boolean enableSimpleDateFormat) {
        this(protoParser, preserveFieldNames, false, enableSimpleDateFormat);
    }

    public MessageToJson(Parser protoParser, boolean preserveFieldNames, boolean wrappedInsideArray, boolean enableSimpleDateFormat) {
        this.protoParser = protoParser;
        this.preserveFieldNames = preserveFieldNames;
        this.wrapInsideArray = wrappedInsideArray;
        this.enableSimpleDateFormat = enableSimpleDateFormat;
        this.gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageJsonSerializer())
                .setExclusionStrategies(createGsonExclusionStrategy())
                .setFieldNamingStrategy(field -> field.getName().replaceAll("_", "")).create();
    }

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

    private JSONObject convertProtoBuffTimeStampToDateTime(JSONObject jsonObject, String parentField,
                                                           String timeStampField) throws ParseException {
        JSONObject parentObject = (JSONObject) new JSONParser().parse(jsonObject.get(parentField).toString());
        String timestampObject = parentObject.get(timeStampField).toString();

        OffsetDateTime dateTime;
        try {
            dateTime = OffsetDateTime.parse(timestampObject);
        } catch (DateTimeParseException ignored) {
            LocalDateTime localDateTime = LocalDateTime.parse(timestampObject, inputFormatter);
            dateTime = localDateTime.atOffset(ZoneOffset.UTC);
        }
        parentObject.put(timeStampField, outputFormatter.format(dateTime));
        jsonObject.put(parentField, gson.toJson(parentObject));

        return jsonObject;
    }

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
