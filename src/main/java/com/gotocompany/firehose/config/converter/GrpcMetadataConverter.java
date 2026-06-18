package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

/**
 * Owner {@link Converter} that parses gRPC call metadata from a delimited configuration string into
 * a key-value map.
 *
 * <p>The input is a comma-separated list of {@code key:value} pairs (for example
 * {@code authorization:token,tenant:acme}). Blank entries are ignored and the key and value of each
 * remaining entry are trimmed. A blank or empty input yields an empty map.
 */
public class GrpcMetadataConverter implements Converter<Map<String, String>> {

    /**
     * Separator between successive {@code key:value} metadata entries.
     */
    private static final String PAIR_DELIMITER = ",";
    /**
     * Separator between a metadata key and its value within an entry.
     */
    private static final String KEY_VALUE_DELIMITER = ":";
    /**
     * Index of the key within a split {@code key:value} entry.
     */
    private static final int KEY_INDEX = 0;
    /**
     * Index of the value within a split {@code key:value} entry.
     */
    private static final int VALUE_INDEX = 1;

    /**
     * Parses the configured metadata string into a map of metadata keys to values.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw metadata string of comma-separated {@code key:value} entries
     * @return a map of the parsed metadata entries, empty when the input is blank
     * @throws IllegalArgumentException if an entry has no {@code :} separator or a blank key
     */
    @Override
    public Map<String, String> convert(Method method, String input) {
        if (StringUtils.isBlank(input)) {
            return new HashMap<>();
        }
        return Arrays.stream(input.split(PAIR_DELIMITER))
                .filter(StringUtils::isNotBlank)
                .map(pair -> {
                    String[] split = pair.split(KEY_VALUE_DELIMITER);
                    if (split.length < 2 || StringUtils.isBlank(split[KEY_INDEX])) {
                        throw new IllegalArgumentException("Invalid metadata entry: " + pair);
                    }
                    return new AbstractMap.SimpleEntry<>(split[KEY_INDEX].trim(), split[VALUE_INDEX].trim());
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

}
