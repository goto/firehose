package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.SerializedNumericTypecastConverter;
import org.aeonbits.owner.Config;

import java.util.Map;
import java.util.function.Function;

public interface SerializerConfig extends Config {
    @Config.Key("SERIALIZER_JSON_NUMERIC_TYPECAST")
    @Config.ConverterClass(SerializedNumericTypecastConverter.class)
    Map<String, Function<String, Number>> serializerJsonNumericTypecast();
}
