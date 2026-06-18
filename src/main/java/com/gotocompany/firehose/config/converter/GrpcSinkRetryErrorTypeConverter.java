package com.gotocompany.firehose.config.converter;

import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Locale;

/**
 * Owner {@link Converter} that resolves a configuration string into a depot
 * {@link com.gotocompany.depot.error.ErrorType}, used to declare which error types make a gRPC sink
 * call retryable.
 *
 * <p>The input is trimmed and upper-cased using {@code Locale.ROOT} before lookup, so values are
 * accepted in any case and tolerate surrounding whitespace. An unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class GrpcSinkRetryErrorTypeConverter implements Converter<ErrorType> {
    /**
     * Converts the raw configuration value into a depot
     * {@link com.gotocompany.depot.error.ErrorType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param s the raw value naming the error type (any case, with optional surrounding whitespace)
     * @return the matching {@link com.gotocompany.depot.error.ErrorType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public ErrorType convert(Method method, String s) {
        return ErrorType.valueOf(s.trim().toUpperCase(Locale.ROOT));
    }
}
