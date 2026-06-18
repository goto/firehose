package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.dlq.DLQWriterType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the dead-letter-queue writer configuration string into a
 * {@link com.gotocompany.firehose.sink.dlq.DLQWriterType} constant.
 *
 * <p>The writer type selects where failed records are parked (for example blob storage, Kafka or
 * the log). The input is upper-cased before lookup; an unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class DlqWriterTypeConverter implements Converter<DLQWriterType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.sink.dlq.DLQWriterType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the DLQ writer (any case)
     * @return the matching {@link com.gotocompany.firehose.sink.dlq.DLQWriterType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public DLQWriterType convert(Method method, String input) {
        return DLQWriterType.valueOf(input.toUpperCase());
    }
}
