package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the Elasticsearch sink input-message-type configuration
 * string into a {@link com.gotocompany.firehose.config.enums.EsSinkMessageType} constant.
 *
 * <p>The input is upper-cased before lookup. This converter performs no extra validation, so an
 * unrecognised value propagates the {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class EsSinkMessageTypeConverter implements Converter<EsSinkMessageType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.EsSinkMessageType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code JSON} or {@code PROTOBUF} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.EsSinkMessageType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public EsSinkMessageType convert(Method method, String input) {
        return EsSinkMessageType.valueOf(input.toUpperCase());
    }
}
