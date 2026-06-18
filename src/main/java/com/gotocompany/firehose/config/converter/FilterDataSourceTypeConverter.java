package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code FILTER_DATA_SOURCE} configuration string into a
 * {@link com.gotocompany.firehose.config.enums.FilterDataSourceType} constant.
 *
 * <p>Matching is case-insensitive because the input is upper-cased before being looked up. An
 * unrecognised value triggers an {@code IllegalArgumentException} whose message names the
 * configuration key.
 */
public class FilterDataSourceTypeConverter implements Converter<FilterDataSourceType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.FilterDataSourceType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code KEY} or {@code MESSAGE} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.FilterDataSourceType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public FilterDataSourceType convert(Method method, String input) {
        try {
            return FilterDataSourceType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_DATA_SOURCE must be or KEY or MESSAGE", e);
        }
    }
}
