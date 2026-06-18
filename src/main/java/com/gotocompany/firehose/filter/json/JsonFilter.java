package com.gotocompany.firehose.filter.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.enums.FilterMessageFormatType;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static com.gotocompany.firehose.config.enums.FilterDataSourceType.KEY;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    /** Common prefix for this filter's StatsD metrics. */
    private static final String METRIC_PREFIX = "firehose_json_filter_";
    /** Metric counting protobuf deserialization failures. */
    private static final String DESERIALIZATION_ERRORS = METRIC_PREFIX + "deserialization_errors_total";

    /** Filter configuration (data source, schema, message format). */
    private final FilterConfig filterConfig;
    /** Records logs and the deserialization-error metric. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Compiled JSON Schema (draft v7) used to validate each message. */
    private final JsonSchema schema;
    /** Maps JSON text to a tree for validation. */
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Prints parsed protobuf messages as JSON; only set for protobuf input. */
    private JsonFormat.Printer jsonPrinter;
    /** Stencil parser for protobuf input; only set for protobuf input. */
    private Parser parser;
    /** Whether deserialization errors are dropped (counted and filtered out) rather than thrown. */
    private final boolean dropDeserializationError;

    /**
     * Instantiates a new Json filter.
     *
     * @param filterConfig    the consumer config
     * @param firehoseInstrumentation the instrumentation
     */
    public JsonFilter(StencilClient stencilClient, FilterConfig filterConfig, FirehoseInstrumentation firehoseInstrumentation) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.filterConfig = filterConfig;
        this.dropDeserializationError = filterConfig.getFilterDropDeserializationError();
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        this.schema = schemaFactory.getSchema(filterConfig.getFilterJsonSchema());
        if (filterConfig.getFilterESBMessageFormat() == FilterMessageFormatType.PROTOBUF) {
            this.parser = stencilClient.getParser(filterConfig.getFilterSchemaProtoClass());
            this.jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
        }
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the json/protobuf records in binary format that are wrapped in {@link Message}
     * @return the list of filtered Messages
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        for (Message message : messages) {
            byte[] data = (filterConfig.getFilterDataSource().equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = deserialize(data);
            if (jsonMessage != null && evaluate(jsonMessage)) {
                filteredMessages.addToValidMessages(message);
            } else {
                filteredMessages.addToInvalidMessages(message);
            }
        }
        return filteredMessages;
    }

    /**
     * Validates a JSON string against the configured schema.
     *
     * @param jsonMessage the JSON document to validate
     * @return {@code true} if the document has no schema validation errors
     * @throws FilterException if the JSON cannot be parsed
     */
    private boolean evaluate(String jsonMessage) throws FilterException {
        try {
            JsonNode message = objectMapper.readTree(jsonMessage);
            if (firehoseInstrumentation.isDebugEnabled()) {
                firehoseInstrumentation.logDebug("Json Message: \n {}", message.toPrettyString());
            }
            Set<ValidationMessage> validationErrors = schema.validate(message);
            validationErrors.forEach(error -> {
                firehoseInstrumentation.logDebug("Message filtered out due to: {}", error.getMessage());
            });
            return validationErrors.isEmpty();
        } catch (JsonProcessingException e) {
            throw new FilterException("Failed to parse JSON message", e);
        }
    }

    /**
     * Converts a record's raw bytes to a JSON string based on the configured message format.
     *
     * <p>For protobuf input the bytes are parsed and printed as JSON; for JSON input the bytes are
     * decoded directly. Returns {@code null} when a droppable protobuf deserialization error occurs.
     *
     * @param data the raw key or value bytes
     * @return the JSON representation, or {@code null} if a droppable deserialization error occurred
     * @throws FilterException if deserialization fails and errors are not dropped, or the format is
     *                         not supported
     */
    private String deserialize(byte[] data) throws FilterException {
        switch (filterConfig.getFilterESBMessageFormat()) {
            case PROTOBUF:
                try {
                    DynamicMessage message = parser.parse(data);
                    return jsonPrinter.print(message);
                } catch (Exception e) {
                    if (dropDeserializationError && (e instanceof InvalidProtocolBufferException || e.getCause() instanceof InvalidProtocolBufferException)) {
                        firehoseInstrumentation.captureCount(DESERIALIZATION_ERRORS, 1L);
                        firehoseInstrumentation.logWarn("Failed to deserialize protobuf message: {}", e.getMessage());
                        return null;
                    } else {
                        throw new FilterException("Failed to parse Protobuf message", e);
                    }
                }
            case JSON:
                return new String(data, Charset.defaultCharset());
            default:
                throw new FilterException("Invalid message format type");
        }
    }
}
