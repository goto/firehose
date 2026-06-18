package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.json.simple.JSONObject;

/**
 * The Mongo update request handler.
 * This class is responsible for creating requests when one
 * or more fields of a MongoDB document need to be updated,
 * if a document with that primary key already exists,
 * otherwise a new document is inserted into the MongoDB
 * collection.
 *
 * @since 0.1
 */
public class MongoUpsertRequestHandler extends MongoRequestHandler {

    /** Resolved request type; this handler is active only for {@link MongoSinkRequestType#UPSERT}. */
    private final MongoSinkRequestType mongoSinkRequestType;
    /** Primary key field used as the document {@code _id}; may be {@code null} for insert-only. */
    private final String mongoPrimaryKey;

    /**
     * Instantiates a new Mongo upsert request handler.
     *
     * @param messageType          the message type
     * @param jsonSerializer       the json serializer
     * @param mongoSinkRequestType the Mongo sink request type, i.e. UPDATE_ONLY/INSERT_OR_UPDATE
     * @param mongoPrimaryKey      the Mongo primary key
     * @since 0.1
     */
    public MongoUpsertRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey, String kafkaRecordParserMode) {
        super(messageType, jsonSerializer, kafkaRecordParserMode);
        this.mongoSinkRequestType = mongoSinkRequestType;
        this.mongoPrimaryKey = mongoPrimaryKey;
    }

    /**
     * Indicates whether this handler applies to the configured request type.
     *
     * @return {@code true} when the configured request type is upsert
     */
    @Override
    public boolean canCreate() {
        return mongoSinkRequestType == MongoSinkRequestType.UPSERT;
    }

    /**
     * Builds the upsert write model for the given message.
     * <p>
     * With no primary key configured an {@code InsertOneModel} is returned; otherwise a
     * {@code ReplaceOneModel} keyed by {@code _id} with upsert enabled is returned.
     *
     * @param message the message to convert
     * @return the MongoDB write model for the message
     */
    @Override
    public WriteModel<Document> getRequest(Message message) {
        String logMessage = extractPayload(message);
        JSONObject logMessageJSONObject = getJSONObject(logMessage);

        Document document;
        if (mongoPrimaryKey == null) {
            document = new Document(logMessageJSONObject);
            return new InsertOneModel<>(document);
        }
        String primaryKeyValue = getFieldFromJSON(logMessageJSONObject, mongoPrimaryKey);
        document = new Document("_id", primaryKeyValue);
        document.putAll(logMessageJSONObject);

        return new ReplaceOneModel<>(
                new Document("_id", primaryKeyValue),
                document,
                new ReplaceOptions().upsert(true));
    }
}
