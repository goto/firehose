package com.gotocompany.firehose.utils;

import java.util.regex.Pattern;

/**
 * Identifies a category of Kafka producer and the environment-variable prefix that configures it.
 *
 * <p>Currently only {@code DLQ} is defined, whose settings are supplied via {@code DLQ_KAFKA_*}
 * environment variables. {@link #getConfigurationPattern()} builds the regex used to extract those
 * variables when assembling producer properties.
 */
public enum KafkaProducerTypesMetadata {
    /** Dead-letter-queue Kafka producer, configured via {@code DLQ_KAFKA_*} variables. */
    DLQ("DLQ_KAFKA_");

    /** Environment-variable prefix that identifies configuration for this producer type. */
    private final String configurationPrefix;

    /**
     * Binds the producer type to its configuration prefix.
     *
     * @param dlqKafka the environment-variable prefix for this producer type
     */
    KafkaProducerTypesMetadata(String dlqKafka) {
        this.configurationPrefix = dlqKafka;
    }

    /**
     * Returns a case-insensitive pattern matching environment variables with this type's prefix.
     *
     * @return a regex that captures the suffix after the configuration prefix
     */
    public Pattern getConfigurationPattern() {
        return Pattern.compile(String.format("^%s(.*)", configurationPrefix), Pattern.CASE_INSENSITIVE);
    }
}
