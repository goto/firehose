package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.FilterDataSourceTypeConverter;
import com.gotocompany.firehose.config.converter.FilterEngineTypeConverter;
import com.gotocompany.firehose.config.converter.FilterMessageFormatTypeConverter;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.config.enums.FilterEngineType;
import com.gotocompany.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Config;

/**
 * Owner configuration that governs Firehose's optional message-filtering layer.
 *
 * <p>It selects the filter engine and supplies what each engine needs: the protobuf schema, the data
 * source (key or message), the JEXL expression or JSON schema, and the event-timestamp field and
 * acceptance window used by the timestamp filter. Records rejected by the active filter are dropped
 * before reaching the sink. Each accessor maps to an environment variable via {@code @Key} and falls
 * back to its {@code @DefaultValue} when unset.
 */
public interface FilterConfig extends Config {

    /**
     * Returns the filter engine to apply, set by {@code FILTER_ENGINE}, converted by
     * {@link com.gotocompany.firehose.config.converter.FilterEngineTypeConverter} and defaulting to
     * {@code NO_OP} (no filtering).
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.FilterEngineType}
     */
    @Key("FILTER_ENGINE")
    @ConverterClass(FilterEngineTypeConverter.class)
    @DefaultValue("NO_OP")
    FilterEngineType getFilterEngine();

    /**
     * Returns the fully-qualified protobuf class used to deserialize messages for filtering, set by
     * {@code FILTER_SCHEMA_PROTO_CLASS}.
     *
     * @return the filter proto schema class name
     */
    @Key("FILTER_SCHEMA_PROTO_CLASS")
    String getFilterSchemaProtoClass();

    /**
     * Returns the format of the payload the filter operates on, set by
     * {@code FILTER_ESB_MESSAGE_FORMAT} and converted by
     * {@link com.gotocompany.firehose.config.converter.FilterMessageFormatTypeConverter}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.FilterMessageFormatType}
     */
    @Key("FILTER_ESB_MESSAGE_FORMAT")
    @ConverterClass(FilterMessageFormatTypeConverter.class)
    FilterMessageFormatType getFilterESBMessageFormat();

    /**
     * Returns whether the filter reads the Kafka key or message, set by {@code FILTER_DATA_SOURCE}
     * and converted by
     * {@link com.gotocompany.firehose.config.converter.FilterDataSourceTypeConverter}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.FilterDataSourceType}
     */
    @Key("FILTER_DATA_SOURCE")
    @ConverterClass(FilterDataSourceTypeConverter.class)
    FilterDataSourceType getFilterDataSource();

    /**
     * Returns the Apache Commons JEXL boolean expression used by the JEXL filter engine, set by
     * {@code FILTER_JEXL_EXPRESSION}.
     *
     * @return the JEXL filter expression
     */
    @Key("FILTER_JEXL_EXPRESSION")
    String getFilterJexlExpression();

    /**
     * Returns the JSON schema used by the JSON filter engine to accept or reject messages, set by
     * {@code FILTER_JSON_SCHEMA}.
     *
     * @return the JSON filter schema
     */
    @Key("FILTER_JSON_SCHEMA")
    String getFilterJsonSchema();

    /**
     * Returns the protobuf field name holding the event timestamp the timestamp filter inspects,
     * set by {@code FILTER_TIMESTAMP_PROTO_FIELD_NAME} and defaulting to {@code event_timestamp}.
     *
     * @return the event-timestamp proto field name
     */
    @Key("FILTER_TIMESTAMP_PROTO_FIELD_NAME")
    @DefaultValue("event_timestamp")
    String getFilterTimestampFieldName();

    /**
     * Indicates whether messages that fail to deserialize during filtering are dropped instead of
     * raising an error, set by {@code FILTER_DROP_DESERIALIZATION_ERROR} and defaulting to
     * {@code false}.
     *
     * @return {@code true} if deserialization errors should be dropped
     */
    @Key("FILTER_DROP_DESERIALIZATION_ERROR")
    @DefaultValue("false")
    Boolean getFilterDropDeserializationError();

    /**
     * Returns how many seconds into the past an event timestamp may be while still passing the
     * timestamp filter, set by {@code FILTER_TIMESTAMP_PAST_WINDOW_SECONDS} and defaulting to
     * {@code 604800} (seven days).
     *
     * @return the allowed past window in seconds
     */
    @Key("FILTER_TIMESTAMP_PAST_WINDOW_SECONDS")
    @DefaultValue("604800")
    Long getFilterTimestampPastWindowSeconds();

    /**
     * Returns how many seconds into the future an event timestamp may be while still passing the
     * timestamp filter, set by {@code FILTER_TIMESTAMP_FUTURE_WINDOW_SECONDS} and defaulting to
     * {@code 604800} (seven days).
     *
     * @return the allowed future window in seconds
     */
    @Key("FILTER_TIMESTAMP_FUTURE_WINDOW_SECONDS")
    @DefaultValue("604800")
    Long getFilterTimestampFutureWindowSeconds();

}
