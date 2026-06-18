package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.InputSchemaType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code INPUT_SCHEMA_DATA_TYPE} configuration string into
 * an {@link com.gotocompany.firehose.config.enums.InputSchemaType} constant.
 *
 * <p>The input is trimmed and upper-cased before lookup, tolerating surrounding whitespace and any
 * letter case. An unrecognised value propagates the {@code IllegalArgumentException} thrown by the
 * enum lookup.
 */
public class InputSchemaTypeConverter implements Converter<InputSchemaType> {
    /**
     * Converts the raw configuration value into an
     * {@link com.gotocompany.firehose.config.enums.InputSchemaType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code PROTOBUF} or {@code JSON} (any case, with
     *     optional surrounding whitespace)
     * @return the matching {@link com.gotocompany.firehose.config.enums.InputSchemaType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public InputSchemaType convert(Method method, String input) {
        return InputSchemaType.valueOf(input.trim().toUpperCase());
    }
}
