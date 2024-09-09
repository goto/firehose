package com.gotocompany.firehose.utils;

import lombok.Getter;

@Getter
public enum KafkaConnectorType {
    DLQ("DLQ_KAFKA");

    private final String configurationPrefix;

    KafkaConnectorType(String dlqKafka) {
        this.configurationPrefix = dlqKafka;
    }
}
