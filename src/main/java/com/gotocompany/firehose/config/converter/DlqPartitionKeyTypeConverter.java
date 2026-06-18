package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the dead-letter-queue partition-key strategy configuration
 * string into a {@link com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType} constant.
 *
 * <p>The strategy controls how records routed to the DLQ are keyed when they are republished. The
 * input is upper-cased before lookup; an unrecognised value propagates the
 * {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class DlqPartitionKeyTypeConverter implements Converter<DlqPartitionKeyType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the partition-key strategy (any case)
     * @return the matching {@link com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public DlqPartitionKeyType convert(Method method, String input) {
        return DlqPartitionKeyType.valueOf(input.toUpperCase());
    }
}
