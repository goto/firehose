package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import org.aeonbits.owner.Config;

import java.util.Properties;

/**
 * Owner configuration for the InfluxDB sink, which writes consumed messages as time-series points.
 *
 * <p>It maps protobuf field and tag indices to InfluxDB field and tag names, identifies the
 * measurement, database and retention policy, locates the event-timestamp field, and supplies the
 * connection URL and credentials. Each accessor maps to an environment variable via {@code @Key}
 * and, where present, falls back to its {@code @DefaultValue}.
 */
public interface InfluxSinkConfig extends AppConfig {
    /**
     * Returns the mapping from protobuf field indices to InfluxDB field names, set by
     * {@code SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING} and parsed from JSON by
     * {@link com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter}.
     *
     * @return the field-name proto-index mapping as a {@code Properties} tree
     */
    @Config.Key("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxFieldNameProtoIndexMapping();

    /**
     * Returns the mapping from protobuf field indices to InfluxDB tag names, set by
     * {@code SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING} and parsed from JSON by
     * {@link com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter}.
     *
     * @return the tag-name proto-index mapping as a {@code Properties} tree
     */
    @Config.Key("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxTagNameProtoIndexMapping();

    /**
     * Returns the InfluxDB measurement that points are written to, set by
     * {@code SINK_INFLUX_MEASUREMENT_NAME}.
     *
     * @return the InfluxDB measurement name
     */
    @Config.Key("SINK_INFLUX_MEASUREMENT_NAME")
    String getSinkInfluxMeasurementName();

    /**
     * Returns the protobuf field index that holds the event timestamp used as the point time, set by
     * {@code SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX}.
     *
     * @return the event-timestamp proto field index
     */
    @Config.Key("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX")
    Integer getSinkInfluxProtoEventTimestampIndex();

    /**
     * Returns the name of the InfluxDB database to write into, set by {@code SINK_INFLUX_DB_NAME}.
     *
     * @return the InfluxDB database name
     */
    @Config.Key("SINK_INFLUX_DB_NAME")
    String getSinkInfluxDbName();

    /**
     * Returns the InfluxDB retention policy applied to written points, set by
     * {@code SINK_INFLUX_RETENTION_POLICY} and defaulting to {@code autogen}.
     *
     * @return the InfluxDB retention policy name
     */
    @Config.Key("SINK_INFLUX_RETENTION_POLICY")
    @DefaultValue("autogen")
    String getSinkInfluxRetentionPolicy();

    /**
     * Returns the InfluxDB server URL to connect to, set by {@code SINK_INFLUX_URL}.
     *
     * @return the InfluxDB URL
     */
    @Config.Key("SINK_INFLUX_URL")
    String getSinkInfluxUrl();

    /**
     * Returns the username used to authenticate to InfluxDB, set by {@code SINK_INFLUX_USERNAME}.
     *
     * @return the InfluxDB username
     */
    @Config.Key("SINK_INFLUX_USERNAME")
    String getSinkInfluxUsername();

    /**
     * Returns the password used to authenticate to InfluxDB, set by {@code SINK_INFLUX_PASSWORD}.
     *
     * @return the InfluxDB password
     */
    @Config.Key("SINK_INFLUX_PASSWORD")
    String getSinkInfluxPassword();
}

