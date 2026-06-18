package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.SinkType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code SINK_TYPE} configuration string into a
 * {@link com.gotocompany.firehose.config.enums.SinkType} constant.
 *
 * <p>The input is upper-cased before lookup. An unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class SinkTypeConverter implements Converter<SinkType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.SinkType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the desired sink (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.SinkType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public SinkType convert(Method method, String input) {
        return SinkType.valueOf(input.toUpperCase());
    }
}
