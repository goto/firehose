package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.SetErrorTypeConverter;
import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.Config;
import org.aeonbits.owner.Mutable;

import java.util.Set;

/**
 * Owner configuration that classifies depot error types into the action Firehose takes for each.
 *
 * <p>It extends the owner {@link org.aeonbits.owner.Config} and {@link org.aeonbits.owner.Mutable}
 * contracts and groups error types into three sets: those routed to the DLQ, those retried, and
 * those that fail the consumer. Each accessor reads a comma-separated list (see {@code @Separator})
 * and converts every token with
 * {@link com.gotocompany.firehose.config.converter.SetErrorTypeConverter}.
 */
public interface ErrorConfig extends Config, Mutable {

    /**
     * Returns the set of error types whose messages are routed to the dead-letter queue, set by
     * {@code ERROR_TYPES_FOR_DLQ} as a comma-separated list and defaulting to an empty set.
     *
     * @return the set of {@link com.gotocompany.depot.error.ErrorType} values handled by the DLQ
     */
    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_DLQ")
    @Separator(",")
    @DefaultValue("")
    Set<ErrorType> getErrorTypesForDLQ();

    /**
     * Returns the set of error types whose messages are retried, set by
     * {@code ERROR_TYPES_FOR_RETRY} as a comma-separated list and defaulting to
     * {@code DEFAULT_ERROR,SINK_RETRYABLE_ERROR}.
     *
     * @return the set of {@link com.gotocompany.depot.error.ErrorType} values that are retried
     */
    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_RETRY")
    @Separator(",")
    @DefaultValue("DEFAULT_ERROR,SINK_RETRYABLE_ERROR")
    Set<ErrorType> getErrorTypesForRetry();

    /**
     * Returns the set of error types that cause the consumer to fail rather than retry or park the
     * message, set by {@code ERROR_TYPES_FOR_FAILING} as a comma-separated list and defaulting to
     * {@code DESERIALIZATION_ERROR,INVALID_MESSAGE_ERROR,UNKNOWN_FIELDS_ERROR}.
     *
     * @return the set of {@link com.gotocompany.depot.error.ErrorType} values that fail the consumer
     */
    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_FAILING")
    @Separator(",")
    @DefaultValue("DESERIALIZATION_ERROR,INVALID_MESSAGE_ERROR,UNKNOWN_FIELDS_ERROR")
    Set<ErrorType> getErrorTypesForFailing();

}
