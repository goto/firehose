package com.gotocompany.firehose.sink.dlq;

public enum DlqPartitionKeyType {
    PRODUCE_TIMESTAMP,
    CONSUME_TIMESTAMP
}
