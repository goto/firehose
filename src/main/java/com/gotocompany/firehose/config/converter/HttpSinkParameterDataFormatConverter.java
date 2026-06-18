package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


/**
 * Owner {@link Converter} that resolves the HTTP sink parameter data-format configuration string
 * into a {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType} constant.
 *
 * <p>This converter shares the {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType}
 * target with
 * {@link com.gotocompany.firehose.config.converter.HttpSinkDataFormatTypeConverter} but is bound to
 * the setting that controls how dynamic request parameters (rather than the request body) are
 * serialized. The input is upper-cased before lookup and an unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class HttpSinkParameterDataFormatConverter implements Converter<HttpSinkDataFormatType> {
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
