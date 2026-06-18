package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.KafkaConsumerMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the {@code SOURCE_KAFKA_CONSUMER_MODE} configuration string
 * into a {@link com.gotocompany.firehose.config.enums.KafkaConsumerMode} constant.
 *
 * <p>The input is upper-cased before lookup, so values such as {@code sync} and {@code SYNC} are
 * treated alike. An unrecognised value propagates the {@code IllegalArgumentException} thrown by the
 * enum lookup.
 */
public class ConsumerModeConverter implements Converter<KafkaConsumerMode> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.config.enums.KafkaConsumerMode}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value, expected to be {@code SYNC} or {@code ASYNC} (any case)
     * @return the matching {@link com.gotocompany.firehose.config.enums.KafkaConsumerMode} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public KafkaConsumerMode convert(Method method, String input) {
        return KafkaConsumerMode.valueOf(input.toUpperCase());
    }
}
