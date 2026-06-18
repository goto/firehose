package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Request handler that builds Elasticsearch update requests.
 * <p>
 * Produces an {@link UpdateRequest} targeting an existing document by id, so only documents that
 * already exist are modified. Applies the optional routing key when configured.
 */
public class EsUpdateRequestHandler extends EsRequestHandler {

    /** Elasticsearch mapping type name applied to the request. */
    private final String esTypeName;
    /** Target Elasticsearch index name. */
    private final String esIndexName;
    /** Resolved request type; this handler is active only for {@link EsSinkRequestType#UPDATE_ONLY}. */
    private EsSinkRequestType esSinkRequestType;
    /** Field name whose value becomes the document {@code _id}. */
    private String esIdFieldName;
    /** Optional routing key field name; when present, sets the request routing. */
    private String esRoutingKeyName;

    /**
     * Creates a new Elasticsearch update request handler.
     *
     * @param messageType       the input message type
     * @param jsonSerializer    the serializer used to convert protobuf messages to JSON
     * @param esTypeName        the Elasticsearch mapping type name
     * @param esIndexName       the target index name
     * @param esSinkRequestType the resolved request type; this handler is active for update-only
     * @param esIdFieldName     the field whose value becomes the document id
     * @param esRoutingKeyName  the optional routing key field name
     */
    public EsUpdateRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer, String esTypeName, String esIndexName, EsSinkRequestType esSinkRequestType, String esIdFieldName, String esRoutingKeyName) {
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
     * @return {@code true} when the configured request type is update-only
     */
    @Override
    public boolean canCreate() {
        return esSinkRequestType == EsSinkRequestType.UPDATE_ONLY;
    }

    /**
     * Builds an Elasticsearch update request for the given message.
     * <p>
     * The document id is taken from the configured id field, the optional routing key is applied, and
     * the JSON payload is set as the update document.
     *
     * @param message the message to convert
     * @return the Elasticsearch update request
     */
    public DocWriteRequest getRequest(Message message) {
        String logMessage = extractPayload(message);
        UpdateRequest request = new UpdateRequest(esIndexName, esTypeName, getFieldFromJSON(logMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(logMessage, esRoutingKeyName));
        }
        request.doc(logMessage, XContentType.JSON);
        return request;
    }
}
