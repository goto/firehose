package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Request handler that builds Elasticsearch index (insert-or-update) requests.
 * <p>
 * Produces an {@link IndexRequest} keyed by document id, which inserts a new document or replaces an
 * existing one. Applies the optional routing key when configured.
 */
public class EsUpsertRequestHandler extends EsRequestHandler {
    /** Elasticsearch mapping type name applied to the request. */
    private final String esTypeName;
    /** Target Elasticsearch index name. */
    private final String esIndexName;
    /** Resolved request type; this handler is active only for {@link EsSinkRequestType#INSERT_OR_UPDATE}. */
    private EsSinkRequestType esSinkRequestType;
    /** Field name whose value becomes the document {@code _id}. */
    private String esIdFieldName;
    /** Optional routing key field name; when present, sets the request routing. */
    private String esRoutingKeyName;

    /**
     * Creates a new Elasticsearch upsert request handler.
     *
     * @param messageType       the input message type
     * @param jsonSerializer    the serializer used to convert protobuf messages to JSON
     * @param esTypeName        the Elasticsearch mapping type name
     * @param esIndexName       the target index name
     * @param esSinkRequestType the resolved request type; this handler is active for insert-or-update
     * @param esIdFieldName     the field whose value becomes the document id
     * @param esRoutingKeyName  the optional routing key field name
     */
    public EsUpsertRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer, String esTypeName, String esIndexName, EsSinkRequestType esSinkRequestType, String esIdFieldName, String esRoutingKeyName) {
        super(messageType, jsonSerializer);
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.esSinkRequestType = esSinkRequestType;
        this.esIdFieldName = esIdFieldName;
        this.esRoutingKeyName = esRoutingKeyName;
    }

    /**
     * Indicates whether this handler applies to the configured request type.
     *
     * @return {@code true} when the configured request type is insert-or-update
     */
    @Override
    public boolean canCreate() {
        return esSinkRequestType == EsSinkRequestType.INSERT_OR_UPDATE;
    }

    /**
     * Builds an Elasticsearch index request for the given message.
     * <p>
     * The document id is taken from the configured id field, the optional routing key is applied, and
     * the JSON payload is set as the document source.
     *
     * @param message the message to convert
     * @return the Elasticsearch index request
     */
    public DocWriteRequest getRequest(Message message) {
        String logMessage = extractPayload(message);
        IndexRequest request = new IndexRequest(esIndexName, esTypeName, getFieldFromJSON(logMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(logMessage, esRoutingKeyName));
        }
        request.source(logMessage, XContentType.JSON);
        return request;
    }
}
