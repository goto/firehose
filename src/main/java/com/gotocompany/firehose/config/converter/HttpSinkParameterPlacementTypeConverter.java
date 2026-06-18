package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the HTTP sink parameter-placement configuration string into
 * a {@link com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class HttpSinkParameterPlacementTypeConverter implements Converter<HttpSinkParameterPlacementType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code QUERY} or {@code HEADER} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public HttpSinkParameterPlacementType convert(Method method, String input) {
        return HttpSinkParameterPlacementType.valueOf(input.toUpperCase());
    }
}
