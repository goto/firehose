package com.gotocompany.firehose.utils;

import lombok.Getter;

import java.util.regex.Pattern;

@Getter
public enum KafkaProducerTypesMetadata {
    DLQ("DLQ_KAFKA_");

    private final String configurationPrefix;

    KafkaProducerTypesMetadata(String dlqKafka) {
        this.configurationPrefix = dlqKafka;
    }

    public Pattern getConfigurationPattern() {
        return Pattern.compile(String.format("^%s(.*)", configurationPrefix), Pattern.CASE_INSENSITIVE);
    }
}
