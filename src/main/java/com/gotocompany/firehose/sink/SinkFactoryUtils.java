package com.gotocompany.firehose.sink;

import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility that augments the raw environment configuration with the additional keys required by
 * Depot-based sink connectors.
 * <p>
 * Firehose exposes some settings under its own environment variable names (for example
 * {@code INPUT_SCHEMA_PROTO_CLASS} and {@code KAFKA_RECORD_PARSER_MODE}). This helper copies and
 * translates those values into the {@code SINK_CONNECTOR_*} keys that the Depot sink factories
 * expect, and applies Firehose-specific defaults such as the {@code firehose_} metrics prefix.
 */
public class SinkFactoryUtils {
    /**
     * Builds the connector configuration map expected by Depot sink factories from the given
     * environment.
     * <p>
     * Copies {@code env} and adds the {@code SINK_CONNECTOR_*} entries derived from Firehose
     * settings: the protobuf message and key classes from {@code INPUT_SCHEMA_PROTO_CLASS}, the
     * schema data type (defaulting to {@code protobuf}), the {@code firehose_} metrics application
     * prefix, the allow-unknown-fields flag, and the schema message mode chosen from
     * {@code KAFKA_RECORD_PARSER_MODE} ({@code LOG_KEY} when the parser mode is {@code key},
     * otherwise {@code LOG_MESSAGE}).
     *
     * @param env the source configuration, typically the process environment variables
     * @return a new map containing the original entries plus the connector-specific keys
     */
    protected static Map<String, String> addAdditionalConfigsForSinkConnectors(Map<String, String> env) {
        Map<String, String> finalConfig = new HashMap<>(env);
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", env.getOrDefault("INPUT_SCHEMA_DATA_TYPE", "protobuf"));
        finalConfig.put("SINK_METRICS_APPLICATION_PREFIX", "firehose_");
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", env.getOrDefault("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false"));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE",
                env.getOrDefault("KAFKA_RECORD_PARSER_MODE", "").equals("key") ? SinkConnectorSchemaMessageMode.LOG_KEY.name() : SinkConnectorSchemaMessageMode.LOG_MESSAGE.name());
        return finalConfig;
    }
}
