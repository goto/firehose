package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.EsSinkMessageTypeConverter;
import com.gotocompany.firehose.config.enums.EsSinkMessageType;


/**
 * Owner configuration for the Elasticsearch sink, which indexes consumed messages into an
 * Elasticsearch cluster.
 *
 * <p>It supplies the connection URLs and timeouts, the target index, type, id and routing fields,
 * the input message format, retry behaviour and the update mode that decides whether documents are
 * updated only or upserted. Each accessor maps to an environment variable via {@code @Key} and,
 * where present, falls back to its {@code @DefaultValue}.
 */
public interface EsSinkConfig extends AppConfig {

    /**
     * Returns the number of shard copies that must be active before an indexing request proceeds,
     * set by {@code SINK_ES_SHARDS_ACTIVE_WAIT_COUNT} and defaulting to {@code 1}.
     *
     * @return the active shard wait count
     */
    @Key("SINK_ES_SHARDS_ACTIVE_WAIT_COUNT")
    @DefaultValue("1")
    Integer getSinkEsShardsActiveWaitCount();

    /**
     * Returns the timeout in milliseconds for an Elasticsearch bulk request, set by
     * {@code SINK_ES_REQUEST_TIMEOUT_MS} and defaulting to {@code 60000}.
     *
     * @return the Elasticsearch request timeout in milliseconds
     */
    @Key("SINK_ES_REQUEST_TIMEOUT_MS")
    @DefaultValue("60000")
    Long getSinkEsRequestTimeoutMs();

    /**
     * Returns the comma-separated HTTP status codes that are never retried, set by
     * {@code SINK_ES_RETRY_STATUS_CODE_BLACKLIST} and defaulting to {@code 404} (not found).
     *
     * @return the blacklisted (non-retryable) status codes
     */
    @Key("SINK_ES_RETRY_STATUS_CODE_BLACKLIST")
    @DefaultValue("404")
    String getSinkEsRetryStatusCodeBlacklist();

    /**
     * Returns the comma-separated Elasticsearch host:port connection URLs, set by
     * {@code SINK_ES_CONNECTION_URLS}.
     *
     * @return the Elasticsearch connection URLs
     */
    @Key("SINK_ES_CONNECTION_URLS")
    String getSinkEsConnectionUrls();

    /**
     * Returns the name of the Elasticsearch index documents are written to, set by
     * {@code SINK_ES_INDEX_NAME}.
     *
     * @return the Elasticsearch index name
     */
    @Key("SINK_ES_INDEX_NAME")
    String getSinkEsIndexName();

    /**
     * Returns the Elasticsearch mapping type used for indexed documents, set by
     * {@code SINK_ES_TYPE_NAME}.
     *
     * @return the Elasticsearch type name
     */
    @Key("SINK_ES_TYPE_NAME")
    String getSinkEsTypeName();

    /**
     * Returns the message field whose value is used as the Elasticsearch document id, set by
     * {@code SINK_ES_ID_FIELD}.
     *
     * @return the document id field name
     */
    @Key("SINK_ES_ID_FIELD")
    String getSinkEsIdField();

    /**
     * Indicates whether the sink only updates existing documents (rather than upserting), set by
     * {@code SINK_ES_MODE_UPDATE_ONLY_ENABLE} and defaulting to {@code false}. This flag drives the
     * choice of {@link com.gotocompany.firehose.config.enums.EsSinkRequestType}.
     *
     * @return {@code true} if update-only mode is enabled
     */
    @Key("SINK_ES_MODE_UPDATE_ONLY_ENABLE")
    @DefaultValue("false")
    Boolean isSinkEsModeUpdateOnlyEnable();

    /**
     * Returns the wire format of the incoming messages, set by {@code SINK_ES_INPUT_MESSAGE_TYPE},
     * converted by {@link com.gotocompany.firehose.config.converter.EsSinkMessageTypeConverter} and
     * defaulting to {@code JSON}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.EsSinkMessageType}
     */
    @Key("SINK_ES_INPUT_MESSAGE_TYPE")
    @ConverterClass(EsSinkMessageTypeConverter.class)
    @DefaultValue("JSON")
    EsSinkMessageType getSinkEsInputMessageType();

    /**
     * Indicates whether original protobuf field names are preserved (rather than camel-cased) when
     * building documents, set by {@code SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE} and defaulting to
     * {@code true}.
     *
     * @return {@code true} if proto field names are preserved
     */
    @Key("SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE")
    @DefaultValue("true")
    Boolean isSinkEsPreserveProtoFieldNamesEnable();

    /**
     * Returns the message field whose value is used as the Elasticsearch routing key, set by
     * {@code SINK_ES_ROUTING_KEY_NAME}.
     *
     * @return the routing key field name
     */
    @Key("SINK_ES_ROUTING_KEY_NAME")
    String getSinkEsRoutingKeyName();
}
