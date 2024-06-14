package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.SerializerConfigConverter;
import org.aeonbits.owner.Config;

import java.util.Map;
import java.util.function.Function;

public interface SerializerConfig extends Config {
    @Config.Key("SERIALIZER_JSON_TYPECAST")
    @Config.ConverterClass(SerializerConfigConverter.class)
    @DefaultValue("{}")
    Map<String, Function<String, Object>> getJsonTypecastMapping();
}
