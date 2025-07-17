package com.gotocompany.firehose.config.converter;

import com.jayway.jsonpath.Option;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;

public class HttpJsonBodyTemplateParseOptionConverter implements Converter<Option> {
    @Override
    public Option convert(Method method, String input) {
        if (isNullOrBlank(input)) {
            return null;
        }
        String normalizedInput = input.trim().toUpperCase();
        try {
            return Option.valueOf(normalizedInput);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid JSONPath option: '%s'. Valid options are: %s",
                            input, getValidOptionsString()), e);
        }
    }

    private boolean isNullOrBlank(String input) {
        return input == null || input.trim().isEmpty();
    }

    private String getValidOptionsString() {
        return Arrays.stream(Option.values())
                .map(Enum::name)
                .collect(Collectors.joining(", "));
    }
}
