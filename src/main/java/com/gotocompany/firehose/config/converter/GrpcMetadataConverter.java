package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

/**
 * Converts SINK_GRPC_METADATA into a map of metadata keys and values.
 * Values starting with {@value #CEL_EXPRESSION_MARKER} are CEL expressions and may contain
 * commas and colons inside brackets or quoted strings. All other values keep the plain
 * comma and colon splitting.
 * Keys, including CEL keys, always end at the first colon and must not contain commas or colons.
 */
public class GrpcMetadataConverter implements Converter<Map<String, String>> {

    private static final String PAIR_DELIMITER = ",";
    private static final String KEY_VALUE_DELIMITER = ":";
    private static final String CEL_EXPRESSION_MARKER = "$";
    private static final String OPENING_BRACKETS = "([{";
    private static final String CLOSING_BRACKETS = ")]}";
    private static final String QUOTES = "\"'";
    private static final char ESCAPE_CHARACTER = '\\';
    private static final char NO_QUOTE = 0;
    private static final int NOT_FOUND = -1;
    private static final int KEY_INDEX = 0;
    private static final int VALUE_INDEX = 1;

    @Override
    public Map<String, String> convert(Method method, String input) {
        if (StringUtils.isBlank(input)) {
            return new HashMap<>();
        }
        return splitPairs(input).stream()
                .filter(StringUtils::isNotBlank)
                .map(this::toEntry)
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private List<String> splitPairs(String input) {
        List<String> pairs = new ArrayList<>();
        int pairStart = 0;
        while (pairStart <= input.length()) {
            int pairEnd = findPairEnd(input, pairStart);
            pairs.add(input.substring(pairStart, pairEnd));
            pairStart = pairEnd + 1;
        }
        return Collections.unmodifiableList(pairs);
    }

    private int findPairEnd(String input, int pairStart) {
        int keyValueDelimiterIndex = input.indexOf(KEY_VALUE_DELIMITER, pairStart);
        int nextPairDelimiterIndex = indexOfOrEnd(input, PAIR_DELIMITER, pairStart);
        if (keyValueDelimiterIndex == NOT_FOUND || keyValueDelimiterIndex > nextPairDelimiterIndex) {
            return nextPairDelimiterIndex;
        }
        int valueStart = keyValueDelimiterIndex + 1;
        if (isCelExpression(input.substring(valueStart))) {
            return findCelExpressionEnd(input, pairStart, valueStart);
        }
        return nextPairDelimiterIndex;
    }

    private int findCelExpressionEnd(String input, int pairStart, int valueStart) {
        int depth = 0;
        char openQuote = NO_QUOTE;
        int index = valueStart;
        while (index < input.length()) {
            char current = input.charAt(index);
            if (openQuote != NO_QUOTE) {
                if (current == ESCAPE_CHARACTER) {
                    index++;
                } else if (current == openQuote) {
                    openQuote = NO_QUOTE;
                }
            } else if (QUOTES.indexOf(current) != NOT_FOUND) {
                openQuote = current;
            } else if (OPENING_BRACKETS.indexOf(current) != NOT_FOUND) {
                depth++;
            } else if (CLOSING_BRACKETS.indexOf(current) != NOT_FOUND) {
                depth--;
                if (depth < 0) {
                    throw unbalancedCelExpression(input.substring(pairStart, index + 1));
                }
            } else if (depth == 0 && PAIR_DELIMITER.indexOf(current) != NOT_FOUND) {
                return index;
            }
            index++;
        }
        if (depth != 0 || openQuote != NO_QUOTE) {
            throw unbalancedCelExpression(input.substring(pairStart));
        }
        return input.length();
    }

    private AbstractMap.SimpleEntry<String, String> toEntry(String pair) {
        int keyValueDelimiterIndex = pair.indexOf(KEY_VALUE_DELIMITER);
        if (keyValueDelimiterIndex != NOT_FOUND && isCelExpression(pair.substring(keyValueDelimiterIndex + 1))) {
            String key = pair.substring(0, keyValueDelimiterIndex);
            if (StringUtils.isBlank(key)) {
                throw new IllegalArgumentException("Invalid metadata entry: " + pair);
            }
            return new AbstractMap.SimpleEntry<>(key.trim(), pair.substring(keyValueDelimiterIndex + 1).trim());
        }
        String[] split = pair.split(KEY_VALUE_DELIMITER);
        if (split.length < 2 || StringUtils.isBlank(split[KEY_INDEX])) {
            throw new IllegalArgumentException("Invalid metadata entry: " + pair);
        }
        return new AbstractMap.SimpleEntry<>(split[KEY_INDEX].trim(), split[VALUE_INDEX].trim());
    }

    private static boolean isCelExpression(String value) {
        return value.trim().startsWith(CEL_EXPRESSION_MARKER);
    }

    private static int indexOfOrEnd(String input, String delimiter, int fromIndex) {
        int index = input.indexOf(delimiter, fromIndex);
        return index == NOT_FOUND ? input.length() : index;
    }

    private static IllegalArgumentException unbalancedCelExpression(String pair) {
        return new IllegalArgumentException(
                "Invalid metadata entry, unbalanced brackets or quotes in CEL expression: " + pair);
    }

}
