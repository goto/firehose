package com.gotocompany.firehose.sink.bigquery;

import java.util.Map;
import java.util.function.Function;

/**
 * Helpers for configuring and running the BigQuery sink.
 * <p>
 * The BigQuery sink is a Depot sink wrapped by {@link com.gotocompany.firehose.sink.GenericSink} and
 * built by {@link com.gotocompany.firehose.sink.SinkFactory}. This utility supplies the row-id
 * generator that gives each row a stable identity and registers the Kafka metadata columns in the
 * sink configuration.
 */
public class BigquerySinkUtils {
    /**
     * Returns a function that builds a stable BigQuery row id from a row's metadata columns.
     * <p>
     * The id joins the row's {@code message_topic}, {@code message_partition} and
     * {@code message_offset} values with underscores, making it unique per Kafka record so that
     * retried writes are idempotent.
     *
     * @return a function mapping a row's column map to its row id
     */
    public static Function<Map<String, Object>, String> getRowIDCreator() {
        return (m -> String.format("%s_%d_%d", m.get("message_topic"), m.get("message_partition"), m.get("message_offset")));
    }

    /**
     * Registers the Kafka metadata columns and their types in the sink configuration.
     * <p>
     * Sets {@code SINK_BIGQUERY_METADATA_COLUMNS_TYPES} so the BigQuery sink adds columns for the
     * message offset, topic, load time, message timestamp and partition.
     *
     * @param config the sink configuration to populate, modified in place
     */
    public static void addMetadataColumns(Map<String, String> config) {
        config.put("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
    }
}
