package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


/**
 * Owner {@link Converter} that resolves the HTTP sink request-method configuration string into a
 * {@link com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class HttpSinkRequestMethodConverter implements Converter<HttpSinkRequestMethodType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code PUT}, {@code POST}, {@code PATCH} or
     *     {@code DELETE} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public HttpSinkRequestMethodType convert(Method method, String input) {
        return HttpSinkRequestMethodType.valueOf(input.toUpperCase());
    }
}
