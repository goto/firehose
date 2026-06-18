package com.gotocompany.firehose.config.converter;

import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a single configuration token into a depot
 * {@link com.gotocompany.depot.error.ErrorType}.
 *
 * <p>Owner applies this converter to each element of a delimited list, so it is used to build the
 * set of error types that drive error handling and retry decisions. Unlike most converters here the
 * lookup is case-sensitive, so the token must match an enum constant name exactly. An unrecognised
 * value propagates the {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class SetErrorTypeConverter implements Converter<ErrorType> {
    /**
     * Converts a single configuration token into a depot
     * {@link com.gotocompany.depot.error.ErrorType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw token naming the error type, matched case-sensitively
     * @return the matching {@link com.gotocompany.depot.error.ErrorType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public ErrorType convert(Method method, String input) {
        return ErrorType.valueOf(input);
    }
}
