package com.gotocompany.firehose.config.converter;

import java.lang.reflect.Method;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.aeonbits.owner.Converter;
import org.aeonbits.owner.Tokenizer;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

/**
 * Owner {@link Converter} and {@link org.aeonbits.owner.Tokenizer} that turns the Stencil
 * schema-registry fetch-headers configuration string into HTTP {@link org.apache.http.Header}
 * objects.
 *
 * <p>As a tokenizer it splits the configured value on commas and keeps only well-formed
 * {@code name:value} entries; as a converter it parses each token into a
 * {@link org.apache.http.message.BasicHeader}. Together they let owner expose the setting as a list
 * of headers attached to schema-registry requests.
 */
public class SchemaRegistryHeadersConverter implements Converter<Header>, Tokenizer {

    /**
     * Parses a single {@code name:value} token into an HTTP header.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input a single header token in {@code name:value} form
     * @return a {@link org.apache.http.message.BasicHeader} built from the trimmed name and value
     * @throws ArrayIndexOutOfBoundsException if the token does not contain a {@code :} separator
     */
    @Override
    public Header convert(Method method, String input) {
        String[] split = input.split(":");
        return new BasicHeader(split[0].trim(), split[1].trim());
    }

    /**
     * Splits the raw configuration value into individual header tokens.
     *
     * <p>The value is split on commas, each candidate is trimmed, and only entries that contain a
     * single {@code :} separating a non-empty name from a non-empty value are retained.
     *
     * @param values the raw comma-separated header configuration string
     * @return an array of validated {@code name:value} header tokens
     * @throws IllegalArgumentException if no valid header token can be extracted
     */
    @Override
    public String[] tokens(String values) {
        String[] headers = Pattern.compile(",").splitAsStream(values).map(String::trim)
                .filter(s -> {
                    String[] args = s.split(":");
                    return args.length == 2 && args[0].trim().length() > 0 && args[1].trim().length() > 0;
                })
                .collect(Collectors.toList())
                .toArray(new String[0]);
        if (headers.length == 0) {
            throw new IllegalArgumentException(String.format("provided headers %s is not valid", values));
        }

        return headers;
    }

}
