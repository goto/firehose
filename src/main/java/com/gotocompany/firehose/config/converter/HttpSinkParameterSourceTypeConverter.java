package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the HTTP sink parameter-source configuration string into a
 * {@link com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType} constant.
 *
 * <p>The input is upper-cased before lookup. This converter adds no validation of its own, so an
 * unrecognised value propagates the {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class HttpSinkParameterSourceTypeConverter implements Converter<HttpSinkParameterSourceType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code KEY}, {@code MESSAGE} or {@code DISABLED}
     *     (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public HttpSinkParameterSourceType convert(Method method, String input) {
        return HttpSinkParameterSourceType.valueOf(input.toUpperCase());
    }
}
