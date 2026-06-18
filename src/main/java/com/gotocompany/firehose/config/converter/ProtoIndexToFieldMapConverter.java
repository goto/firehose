package com.gotocompany.firehose.config.converter;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Owner {@link org.aeonbits.owner.Converter} that parses the
 * {@code INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING} JSON configuration into a nested
 * {@link java.util.Properties} tree.
 *
 * <p>The input is a JSON object whose string values map protobuf field indices to output column
 * names, and whose nested objects describe nested messages. String entries become property values
 * and nested objects become nested {@code Properties}; values of any other JSON type are ignored.
 * After the tree is built the converter validates that no column name is mapped more than once.
 *
 * <p>A {@code null} or empty input resolves to {@code null}. Malformed JSON causes the underlying
 * Gson parser to throw.
 */
public class ProtoIndexToFieldMapConverter implements org.aeonbits.owner.Converter<Properties> {
    /**
     * Parses the JSON mapping configuration into a nested {@link java.util.Properties} tree.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the JSON mapping string; a {@code null} or empty value yields {@code null}
     * @return the parsed nested {@link java.util.Properties}, or {@code null} when the input is empty
     * @throws IllegalArgumentException if the same column name appears more than once in the mapping
     */
    @Override
    public Properties convert(Method method, String input) {
        if (Strings.isNullOrEmpty(input)) {
            return null;
        }
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> m = new Gson().fromJson(input, type);
        Properties properties = getProperties(m);
        validate(properties);
        return properties;
    }

    /**
     * Recursively converts a decoded JSON object into a {@link java.util.Properties} tree.
     *
     * <p>String values are copied as-is and nested maps are converted into nested {@code Properties};
     * values of any other type are skipped.
     *
     * @param inputMap the decoded JSON object for the current nesting level
     * @return the {@link java.util.Properties} representation of {@code inputMap}
     */
    private Properties getProperties(Map<String, Object> inputMap) {
        Properties properties = new Properties();
        for (String key : inputMap.keySet()) {
            Object value = inputMap.get(key);
            if (value instanceof String) {
                properties.put(key, value);
            } else if (value instanceof Map) {
                properties.put(key, getProperties((Map) value));
            }
        }
        return properties;
    }

    /**
     * Ensures that no output column name is mapped more than once anywhere in the tree.
     *
     * @param properties the fully built mapping tree to check
     * @throws IllegalArgumentException if one or more column names are duplicated
     */
    private void validate(Properties properties) {
        DuplicateFinder duplicateFinder = flattenValues(properties)
                .collect(DuplicateFinder::new, DuplicateFinder::accept, DuplicateFinder::combine);
        if (duplicateFinder.duplicates.size() > 0) {
            throw new IllegalArgumentException("duplicates found in INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING for : " + duplicateFinder.duplicates);
        }
    }

    /**
     * Recursively streams every leaf string value contained in the given properties tree.
     *
     * @param properties the properties tree to flatten
     * @return a stream of all leaf string values, descending into any nested {@code Properties}
     */
    private Stream<String> flattenValues(Properties properties) {
        return properties
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .flatMap(v -> {
                    if (v instanceof String) {
                        return Stream.of((String) v);
                    } else if (v instanceof Properties) {
                        return flattenValues((Properties) v);
                    } else {
                        return Stream.empty();
                    }
                });
    }

    /**
     * Accumulator that records which string values are encountered more than once while a stream of
     * column names is consumed, supporting use as a parallel-stream collector.
     */
    private class DuplicateFinder implements Consumer<String> {
        /**
         * Set of values seen exactly once so far, used to detect repeats.
         */
        private Set<String> processedValues = new HashSet<>();
        /**
         * Values that have been encountered more than once.
         */
        private List<String> duplicates = new ArrayList<>();

        /**
         * Records the given value, adding it to the duplicates list if it has already been seen.
         *
         * @param o the value to accumulate
         */
        @Override
        public void accept(String o) {
            if (processedValues.contains(o)) {
                duplicates.add(o);
            } else {
                processedValues.add(o);
            }
        }

        /**
         * Merges another accumulator into this one, flagging any value seen in both as a duplicate.
         *
         * @param other the accumulator whose processed values are merged into this instance
         */
        void combine(DuplicateFinder other) {
            other.processedValues
                    .forEach(v -> {
                        if (processedValues.contains(v)) {
                            duplicates.add(v);
                        } else {
                            processedValues.add(v);
                        }
                    });
        }
    }
}
