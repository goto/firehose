package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.FilterDataSourceTypeConverter;
import com.gotocompany.firehose.config.converter.FilterEngineTypeConverter;
import com.gotocompany.firehose.config.converter.FilterMessageFormatTypeConverter;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.config.enums.FilterEngineType;
import com.gotocompany.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Config;

public interface FilterConfig extends Config {

    @Key("FILTER_ENGINE")
    @ConverterClass(FilterEngineTypeConverter.class)
    @DefaultValue("NO_OP")
    FilterEngineType getFilterEngine();

    @Key("FILTER_SCHEMA_PROTO_CLASS")
    String getFilterSchemaProtoClass();

    @Key("FILTER_ESB_MESSAGE_FORMAT")
    @ConverterClass(FilterMessageFormatTypeConverter.class)
    FilterMessageFormatType getFilterESBMessageFormat();

    @Key("FILTER_DATA_SOURCE")
    @ConverterClass(FilterDataSourceTypeConverter.class)
    FilterDataSourceType getFilterDataSource();

    @Key("FILTER_JEXL_EXPRESSION")
    String getFilterJexlExpression();

    @Key("FILTER_JSON_SCHEMA")
    String getFilterJsonSchema();

    @Key("FILTER_TIMESTAMP_PROTO_FIELD_NAME")
    @DefaultValue("event_timestamp")
    String getFilterTimestampFieldName();

    @Key("FILTER_DROP_DESERIALIZATION_ERROR")
    @DefaultValue("false")
    Boolean getFilterDropDeserializationError();

    @Key("FILTER_TIMESTAMP_PAST_WINDOW_SECONDS")
    @DefaultValue("604800")
    Long getFilterTimestampPastWindowSeconds();

    @Key("FILTER_TIMESTAMP_FUTURE_WINDOW_SECONDS")
    @DefaultValue("604800")
    Long getFilterTimestampFutureWindowSeconds();

}
