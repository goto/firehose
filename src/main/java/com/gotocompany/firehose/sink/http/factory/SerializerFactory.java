package com.gotocompany.firehose.sink.http.factory;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.JsonWrappedProtoByte;
import com.gotocompany.firehose.serializer.MessageSerializer;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.gotocompany.firehose.serializer.MessageToTemplatizedJson;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.serializer.TypecastedJsonSerializer;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import lombok.AllArgsConstructor;

/**
 * SerializerFactory create json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

    /** Bound HTTP sink configuration that drives serializer selection. */
    private HttpSinkConfig httpSinkConfig;
    /** Stencil client used to resolve the protobuf parser for JSON conversion. */
    private StencilClient stencilClient;
    /** Reporter used to instrument templatized JSON serialization. */
    private StatsDReporter statsDReporter;

    /**
     * Builds the serializer matching the configured data format and schema.
     *
     * <p>When no input proto schema is configured, or the data format is {@code PROTO}, a JSON-wrapped proto
     * byte serializer is returned; when the data format is {@code JSON} the proto is converted to JSON,
     * optionally through a body template, and wrapped so configured fields can be typecast.
     *
     * @return the serializer matching the configuration
     */
    public MessageSerializer build() {
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            Parser protoParser = stencilClient.getParser(httpSinkConfig.getInputSchemaProtoClass());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return getTypecastedJsonSerializer(new MessageToJson(protoParser, false, httpSinkConfig.getSinkHttpSimpleDateFormatEnable()));
            } else {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return getTypecastedJsonSerializer(
                        MessageToTemplatizedJson.create(new FirehoseInstrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser, httpSinkConfig.getSinkHttpJsonBodyTemplateParseOption()));
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    /**
     * Wraps a JSON serializer so that fields listed in the configuration are typecast.
     *
     * @param messageSerializer the JSON serializer to wrap
     * @return the typecasting serializer
     */
    private MessageSerializer getTypecastedJsonSerializer(MessageSerializer messageSerializer) {
        return new TypecastedJsonSerializer(messageSerializer, httpSinkConfig);
    }

    /**
     * Reports whether the input proto schema class is absent.
     *
     * @return {@code true} when no input proto schema class is configured
     */
    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getInputSchemaProtoClass() == null || httpSinkConfig.getInputSchemaProtoClass().equals("");
    }
}
