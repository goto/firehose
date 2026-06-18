package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.EsSinkConfig;
import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

/**
 * Factory that selects the appropriate {@link EsRequestHandler} for the configured write mode.
 * <p>
 * Returns an {@link EsUpdateRequestHandler} when update-only mode is enabled, otherwise an
 * {@link EsUpsertRequestHandler}.
 */
@AllArgsConstructor
public class EsRequestHandlerFactory {

    /** The Elasticsearch sink configuration driving the handler selection. */
    private EsSinkConfig esSinkConfig;
    /** Instrumentation used to log the chosen request mode. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Document id field name used to derive each document's {@code _id}. */
    private final String esIdFieldName;
    /** Input message type (JSON or Protobuf) passed to the created handler. */
    private final EsSinkMessageType messageType;
    /** Serializer used by the created handler to convert messages to JSON. */
    private final MessageToJson jsonSerializer;
    /** Elasticsearch mapping type name applied to each request. */
    private final String esTypeName;
    /** Target Elasticsearch index name. */
    private final String esIndexName;
    /** Optional routing key field name; when set, routes each document. */
    private final String esRoutingKeyName;

    /**
     * Returns the request handler matching the configured write mode.
     * <p>
     * Selects update-only or insert-or-update based on {@code SINK_ES_MODE_UPDATE_ONLY_ENABLE} and
     * returns the first candidate handler whose {@link EsRequestHandler#canCreate()} matches.
     *
     * @return the selected {@link EsRequestHandler}
     * @throws IllegalArgumentException if the resolved request type is unsupported
     */
    public EsRequestHandler getRequestHandler() {
        EsSinkRequestType esSinkRequestType = esSinkConfig.isSinkEsModeUpdateOnlyEnable() ? EsSinkRequestType.UPDATE_ONLY : EsSinkRequestType.INSERT_OR_UPDATE;
        firehoseInstrumentation.logInfo("ES request mode: {}", esSinkRequestType);

        ArrayList<EsRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new EsUpdateRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));
        esRequestHandlers.add(new EsUpsertRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));

        return esRequestHandlers
                .stream()
                .filter(EsRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esSinkRequestType.name() + " not supported"));
    }
}
