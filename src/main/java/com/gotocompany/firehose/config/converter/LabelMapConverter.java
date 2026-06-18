package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Owner {@link Converter} that parses a delimited list of label assignments into an ordered
 * key-value map.
 *
 * <p>The input is a comma-separated list of {@code key=value} pairs (for example
 * {@code team=data,env=prod}). Keys and values are trimmed; an entry without a {@code =} separator
 * or with an empty key is skipped, and any value longer than 63 characters is truncated to that
 * length. Insertion order is preserved because the result is a {@code LinkedHashMap}.
 */
public class LabelMapConverter implements Converter<Map<String, String>> {
    /**
     * Separator placed between successive {@code key=value} label entries in the configured string.
     */
    public static final String ELEMENT_SEPARATOR = ",";
    /**
     * Separator between a label key and its value within a single entry.
     */
    private static final String VALUE_SEPARATOR = "=";
    /**
     * Maximum number of characters retained for a label value; longer values are truncated.
     */
    private static final int MAX_LENGTH = 63;

    /**
     * Parses the configured label string into an ordered map of trimmed key-value pairs.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw label string of comma-separated {@code key=value} entries
     * @return a {@code LinkedHashMap} of the parsed labels in their original order, empty when no
     *     valid entry is present
     */
    public Map<String, String> convert(Method method, String input) {
        Map<String, String> result = new LinkedHashMap<>();
        String[] chunks = input.split(ELEMENT_SEPARATOR, -1);
        for (String chunk : chunks) {
            String[] entry = chunk.split(VALUE_SEPARATOR, -1);
            if (entry.length <= 1) {
                continue;
            }
            String key = entry[0].trim();
            if (key.isEmpty()) {
                continue;
            }

            String value = entry[1].trim();
            value = value.length() > MAX_LENGTH ? value.substring(0, MAX_LENGTH) : value;
            result.put(key, value);
        }
        return result;
    }
}

