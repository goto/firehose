package com.gotocompany.firehose.config.enums;

/**
 * Selects the source of the dynamic fields the HTTP sink injects into request URLs or headers.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.HttpSinkParameterSourceTypeConverter} and is
 * used together with {@link HttpSinkParameterPlacementType} to build parameterized requests. When
 * the source is {@code DISABLED} a plain (non-parameterized) request is created instead.
 */
public enum HttpSinkParameterSourceType {
    /**
     * Source the parameter fields from the Kafka message's log key.
     */
    KEY,
    /**
     * Source the parameter fields from the Kafka message's log body.
     */
    MESSAGE,
    /**
     * Do not add any dynamic parameters to the request.
     */
    DISABLED
}
