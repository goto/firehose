package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the HTTP sink body data-format configuration string into a
 * {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class HttpSinkDataFormatTypeConverter implements Converter<HttpSinkDataFormatType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code PROTO} or {@code JSON} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public HttpSinkDataFormatType convert(Method method, String input) {
        return HttpSinkDataFormatType.valueOf(input.toUpperCase());
    }
}
