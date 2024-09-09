package com.gotocompany.firehose.utils;

public enum KafkaConnectorType {
    DLQ("DLQ_KAFKA");

    public final String configurationPrefix;

    KafkaConnectorType(String dlqKafka) {
        this.configurationPrefix = dlqKafka;
    }
}
