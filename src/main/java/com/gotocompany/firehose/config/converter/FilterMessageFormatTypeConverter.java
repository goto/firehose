package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code FILTER_INPUT_MESSAGE_TYPE} configuration string
 * into a {@link com.gotocompany.firehose.config.enums.FilterMessageFormatType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value results in an
 * {@code IllegalArgumentException} naming the configuration key (without a chained cause).
 */
public class FilterMessageFormatTypeConverter implements Converter<FilterMessageFormatType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.FilterMessageFormatType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code JSON} or {@code PROTOBUF} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.FilterMessageFormatType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public FilterMessageFormatType convert(Method method, String input) {
        try {
            return FilterMessageFormatType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_INPUT_MESSAGE_TYPE must be JSON or PROTOBUF");
        }
    }
}
