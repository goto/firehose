package com.gotocompany.firehose.config;


import com.gotocompany.firehose.config.converter.MongoSinkMessageTypeConverter;
import com.gotocompany.firehose.config.enums.MongoSinkMessageType;

/**
 * Owner configuration for the MongoDB sink, which writes consumed messages as documents into a
 * MongoDB collection.
 *
 * <p>It supplies the connection URLs and timeouts, the target database and collection, optional
 * authentication, the input message format, retry behaviour, and the primary key and update mode
 * that decide whether documents are updated only or upserted. Each accessor maps to an environment
 * variable via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface MongoSinkConfig extends AppConfig {

    /**
     * Returns the timeout in milliseconds for establishing a connection to MongoDB, set by
     * {@code SINK_MONGO_CONNECT_TIMEOUT_MS} and defaulting to {@code 30000}.
     *
     * @return the MongoDB connect timeout in milliseconds
     */
    @Key("SINK_MONGO_CONNECT_TIMEOUT_MS")
    @DefaultValue("30000")
    int getSinkMongoConnectTimeoutMs();

    /**
     * Returns the comma-separated MongoDB host:port connection URLs, set by
     * {@code SINK_MONGO_CONNECTION_URLS}.
     *
     * @return the MongoDB connection URLs
     */
    @Key("SINK_MONGO_CONNECTION_URLS")
    String getSinkMongoConnectionUrls();

    /**
     * Returns the name of the MongoDB database documents are written to, set by
     * {@code SINK_MONGO_DB_NAME}.
     *
     * @return the MongoDB database name
     */
    @Key("SINK_MONGO_DB_NAME")
    String getSinkMongoDBName();

    /**
     * Returns the comma-separated MongoDB error codes that are never retried, set by
     * {@code SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST} and defaulting to {@code 11000} (the duplicate
     * key error).
     *
     * @return the blacklisted (non-retryable) MongoDB error codes
     */
    @Key("SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST")
    @DefaultValue("11000")
    String getSinkMongoRetryStatusCodeBlacklist();

    /**
     * Indicates whether original protobuf field names are preserved (rather than camel-cased) when
     * building documents, set by {@code SINK_MONGO_PRESERVE_PROTO_FIELD_NAMES_ENABLE} and defaulting
     * to {@code true}.
     *
     * @return {@code true} if proto field names are preserved
     */
    @Key("SINK_MONGO_PRESERVE_PROTO_FIELD_NAMES_ENABLE")
    @DefaultValue("true")
    Boolean isSinkMongoPreserveProtoFieldNamesEnable();

    /**
     * Indicates whether MongoDB authentication is used, set by {@code SINK_MONGO_AUTH_ENABLE} and
     * defaulting to {@code false}.
     *
     * @return {@code true} if MongoDB authentication is enabled
     */
    @Key("SINK_MONGO_AUTH_ENABLE")
    @DefaultValue("false")
    Boolean isSinkMongoAuthEnable();

    /**
     * Returns the username used when MongoDB authentication is enabled, set by
     * {@code SINK_MONGO_AUTH_USERNAME}.
     *
     * @return the MongoDB authentication username
     */
    @Key("SINK_MONGO_AUTH_USERNAME")
    String getSinkMongoAuthUsername();

    /**
     * Returns the password used when MongoDB authentication is enabled, set by
     * {@code SINK_MONGO_AUTH_PASSWORD}.
     *
     * @return the MongoDB authentication password
     */
    @Key("SINK_MONGO_AUTH_PASSWORD")
    String getSinkMongoAuthPassword();

    /**
     * Returns the authentication database used to validate the MongoDB credentials, set by
     * {@code SINK_MONGO_AUTH_DB}.
     *
     * @return the MongoDB authentication database name
     */
    @Key("SINK_MONGO_AUTH_DB")
    String getSinkMongoAuthDB();

    /**
     * Returns the wire format of the incoming messages, set by {@code SINK_MONGO_INPUT_MESSAGE_TYPE},
     * converted by {@link com.gotocompany.firehose.config.converter.MongoSinkMessageTypeConverter}
     * and defaulting to {@code JSON}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.MongoSinkMessageType}
     */
    @Key("SINK_MONGO_INPUT_MESSAGE_TYPE")
    @ConverterClass(MongoSinkMessageTypeConverter.class)
    @DefaultValue("JSON")
    MongoSinkMessageType getSinkMongoInputMessageType();

    /**
     * Returns the MongoDB collection documents are written to, set by
     * {@code SINK_MONGO_COLLECTION_NAME}.
     *
     * @return the MongoDB collection name
     */
    @Key("SINK_MONGO_COLLECTION_NAME")
    String getSinkMongoCollectionName();

    /**
     * Returns the document field used as the primary key (the MongoDB {@code _id}) for updates and
     * upserts, set by {@code SINK_MONGO_PRIMARY_KEY}; required when update-only mode is enabled.
     *
     * @return the MongoDB primary key field name
     */
    @Key("SINK_MONGO_PRIMARY_KEY")
    String getSinkMongoPrimaryKey();

    /**
     * Indicates whether the sink only updates existing documents (rather than upserting), set by
     * {@code SINK_MONGO_MODE_UPDATE_ONLY_ENABLE} and defaulting to {@code false}. This flag drives
     * the choice of {@link com.gotocompany.firehose.config.enums.MongoSinkRequestType}.
     *
     * @return {@code true} if update-only mode is enabled
     */
    @Key("SINK_MONGO_MODE_UPDATE_ONLY_ENABLE")
    @DefaultValue("false")
    Boolean isSinkMongoModeUpdateOnlyEnable();

    /**
     * Returns the timeout in milliseconds the driver waits while selecting a MongoDB server, set by
     * {@code SINK_MONGO_SERVER_SELECT_TIMEOUT_MS} and defaulting to {@code 30000}.
     *
     * @return the MongoDB server-selection timeout in milliseconds
     */
    @Key("SINK_MONGO_SERVER_SELECT_TIMEOUT_MS")
    @DefaultValue("30000")
    int getSinkMongoServerSelectTimeoutMs();
}
