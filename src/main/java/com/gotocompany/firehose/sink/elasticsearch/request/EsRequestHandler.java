package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;

/**
 * Base class for Elasticsearch write-request handlers.
 * <p>
 * Responsible for extracting the JSON payload from a {@link Message} (serializing protobuf input to
 * JSON when needed) so concrete handlers can build an Elasticsearch {@code DocWriteRequest}. Concrete
 * subclasses are {@link EsUpdateRequestHandler} and {@link EsUpsertRequestHandler}; the appropriate
 * one is selected by {@link EsRequestHandlerFactory}.
 */
public abstract class EsRequestHandler {
    /** Input message encoding (JSON or Protobuf), controlling how the payload is extracted. */
    private final EsSinkMessageType messageType;
    /** Serializer used to convert a protobuf message into JSON. */
    private final MessageToJson jsonSerializer;
    /** Parser used to read field values out of the JSON payload. */
    private final JSONParser jsonParser;

    /**
     * Creates a new Elasticsearch request handler.
     *
     * @param messageType    the input message type, JSON or Protobuf
     * @param jsonSerializer the serializer used to convert protobuf messages to JSON
     */
    public EsRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer) {
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.jsonParser = new JSONParser();
    }

    /**
     * Indicates whether this handler applies to the configured request type.
     *
     * @return {@code true} if this handler should build the requests
     */
    public abstract boolean canCreate();

    /**
     * Builds the Elasticsearch write request for the given message.
     *
     * @param message the message to convert
     * @return the Elasticsearch write request
     */
    public abstract DocWriteRequest getRequest(Message message);

    /**
     * Extracts the JSON payload from the message.
     * <p>
     * For protobuf input the message is serialized to JSON and the {@code logMessage} field is
     * returned; for JSON input the raw log message bytes are decoded directly.
     *
     * @param message the message to read
     * @return the JSON payload string
     */
    String extractPayload(Message message) {
        if (messageType.equals(EsSinkMessageType.PROTOBUF)) {
            return getFieldFromJSON(jsonSerializer.serialize(message), "logMessage");
        }
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    /**
     * Returns the value of the given key from the JSON string.
     *
     * @param jsonString the JSON document to parse
     * @param key        the key whose value should be returned
     * @return the value at the key, as a string
     * @throws IllegalArgumentException if the key is not present in the document
     * @throws JsonParseException       if the string is not valid JSON
     */
    String getFieldFromJSON(String jsonString, String key) {
        try {
            JSONObject parse = (JSONObject) jsonParser.parse(jsonString);
            Object valueAtKey = parse.get(key);
            if (valueAtKey == null) {
                throw new IllegalArgumentException("Key: " + key + " not found in ESB Message");
            }
            return valueAtKey.toString();
        } catch (ParseException e) {
            throw new JsonParseException(e.getMessage(), e.getCause());
        } finally {
            jsonParser.reset();
        }
    }

}
