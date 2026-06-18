package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.mongodb.client.model.ReplaceOneModel;
import org.bson.Document;
import org.json.simple.JSONObject;

/**
 * The Mongo update request handler.
 * This class is responsible for creating requests when one
 * or more fields of a MongoDB document need to be updated.
 */
public class MongoUpdateRequestHandler extends MongoRequestHandler {

    /** Resolved request type; this handler is active only for {@link MongoSinkRequestType#UPDATE_ONLY}. */
    private final MongoSinkRequestType mongoSinkRequestType;
    /** Primary key field used to match the document to update. */
    private final String mongoPrimaryKey;

    /**
     * Instantiates a new Mongo update request handler.
     *
     * @param messageType          the message type
     * @param jsonSerializer       the json serializer
     * @param mongoSinkRequestType the mongo sink request type
     * @param mongoPrimaryKey      the mongo primary key
     * @since 0.1
     */
    public MongoUpdateRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey, String kafkaRecordParserMode) {
        super(messageType, jsonSerializer, kafkaRecordParserMode);
        this.mongoSinkRequestType = mongoSinkRequestType;
        this.mongoPrimaryKey = mongoPrimaryKey;
    }

    /**
     * Indicates whether this handler applies to the configured request type.
     *
     * @return {@code true} when the configured request type is update-only
     */
    @Override
    public boolean canCreate() {
        return mongoSinkRequestType == MongoSinkRequestType.UPDATE_ONLY;
    }

    /**
     * Builds the update-only write model for the given message.
     * <p>
     * Returns a {@code ReplaceOneModel} keyed by {@code _id} without upsert, so the write only applies
     * when a document with the primary key already exists.
     *
     * @param message the message to convert
     * @return the MongoDB replace write model for the message
     */
    @Override
    public ReplaceOneModel<Document> getRequest(Message message) {
        String logMessage = extractPayload(message);
        JSONObject logMessageJSONObject = getJSONObject(logMessage);
        String primaryKeyValue;

        primaryKeyValue = getFieldFromJSON(logMessageJSONObject, mongoPrimaryKey);
        Document document = new Document("_id", primaryKeyValue);
        document.putAll(logMessageJSONObject);

        return new ReplaceOneModel<>(new Document("_id", primaryKeyValue), document);
    }
}
