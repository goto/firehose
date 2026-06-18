package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.FilterEngineType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code FILTER_ENGINE} configuration string into a
 * {@link com.gotocompany.firehose.config.enums.FilterEngineType} constant.
 *
 * <p>The input is upper-cased before lookup so the value is accepted in any case. When the value
 * does not match a known engine, an {@code IllegalArgumentException} naming the configuration key is
 * raised with the original error attached as its cause.
 */
public class FilterEngineTypeConverter implements Converter<FilterEngineType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.FilterEngineType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be one of {@code JEXL}, {@code JSON}, {@code NO_OP} or
     *     {@code TIMESTAMP} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.FilterEngineType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public FilterEngineType convert(Method method, String input) {
        try {
            return FilterEngineType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_ENGINE must be JSON or JEXL or NOOP", e);
        }
    }
}
