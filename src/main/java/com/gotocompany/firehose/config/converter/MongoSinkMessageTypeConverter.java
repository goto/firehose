package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code SINK_MONGO_INPUT_MESSAGE_TYPE} configuration
 * string into a {@link com.gotocompany.firehose.config.enums.MongoSinkMessageType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value results in an
 * {@code IllegalArgumentException} naming the configuration key.
 */
public class MongoSinkMessageTypeConverter implements Converter<MongoSinkMessageType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.MongoSinkMessageType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code JSON} or {@code PROTOBUF} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.MongoSinkMessageType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public MongoSinkMessageType convert(Method method, String input) {
        try {
            return MongoSinkMessageType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("SINK_MONGO_INPUT_MESSAGE_TYPE must be JSON or PROTOBUF");
        }
    }
}
