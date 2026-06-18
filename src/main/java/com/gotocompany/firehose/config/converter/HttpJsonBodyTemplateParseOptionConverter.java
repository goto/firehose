package com.gotocompany.firehose.config.converter;

import com.jayway.jsonpath.Option;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Owner {@link Converter} that resolves a JSONPath parsing option for the HTTP sink JSON body
 * template into a {@link com.jayway.jsonpath.Option}.
 *
 * <p>A blank or unset value resolves to {@code null}, meaning no option is applied. Otherwise the
 * input is trimmed and upper-cased before being matched against the option names; an unrecognised
 * value raises an {@code IllegalArgumentException} whose message lists the valid options.
 */
public class HttpJsonBodyTemplateParseOptionConverter implements Converter<Option> {
    /**
     * Converts the configured value into a {@link com.jayway.jsonpath.Option}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw option name; a {@code null} or blank value yields {@code null}
     * @return the matching {@link com.jayway.jsonpath.Option}, or {@code null} when the input is blank
     * @throws IllegalArgumentException if a non-blank value does not name a valid option
     */
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

    /**
     * Indicates whether the supplied value is {@code null} or contains only whitespace.
     *
     * @param input the value to test
     * @return {@code true} if the value is {@code null} or blank, otherwise {@code false}
     */
    private boolean isNullOrBlank(String input) {
        return input == null || input.trim().isEmpty();
    }

    /**
     * Builds a comma-separated list of the valid JSONPath option names for use in error messages.
     *
     * @return the names of every {@link com.jayway.jsonpath.Option} constant joined by commas
     */
    private String getValidOptionsString() {
        return Arrays.stream(Option.values())
                .map(Enum::name)
                .collect(Collectors.joining(", "));
    }
}
