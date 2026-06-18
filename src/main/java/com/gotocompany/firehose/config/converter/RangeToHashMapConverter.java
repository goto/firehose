package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Owner {@link Converter} that expands a textual description of numeric ranges into a lookup map.
 *
 * <p>The input is a comma-separated list of inclusive ranges, each written as {@code start-end} (for
 * example {@code 400-600,502-502}). Every integer covered by any range becomes a key in the
 * returned map, mapped to {@code Boolean.TRUE}; integers outside the ranges are simply absent. This
 * is typically used to expand HTTP status-code ranges into a fast membership lookup.
 *
 * <p>Each bound is parsed with {@code Integer.parseInt}, so non-numeric bounds raise a
 * {@code NumberFormatException}, and a range that does not contain two dash-separated bounds raises
 * an {@code IndexOutOfBoundsException}.
 */
public class RangeToHashMapConverter implements Converter<Map<Integer, Boolean>> {

    /**
     * Converts a comma-separated list of inclusive numeric ranges into a membership map.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the range specification, for example {@code 400-600,502-502}
     * @return a map whose keys are every integer covered by the supplied ranges, each mapped to
     *     {@code Boolean.TRUE}
     * @throws NumberFormatException if any range bound is not a valid integer
     * @throws IndexOutOfBoundsException if any entry does not provide both a start and an end bound
     */
    @Override
    public Map<Integer, Boolean> convert(Method method, String input) {
        String[] ranges = input.split(",");
        Map<Integer, Boolean> statusMap = new HashMap<Integer, Boolean>();

        Arrays.stream(ranges).forEach(range -> {
            List<Integer> rangeList = Arrays.stream(range.split("-")).map(Integer::parseInt).collect(Collectors.toList());
            IntStream.rangeClosed(rangeList.get(0), rangeList.get(1)).forEach(statusCode -> statusMap.put(statusCode, true));
        });
        return statusMap;
    }
}
