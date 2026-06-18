package com.gotocompany.firehose.config.enums;

/**
 * Identifies which portion of a Kafka record a Firehose filter evaluates its predicate against.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.FilterDataSourceTypeConverter} and read by the
 * filter implementations (such as the JEXL, JSON and timestamp filters) to decide which bytes of
 * the record are handed to the filter expression.
 *
 * <ul>
 *   <li>{@code KEY} - evaluate the filter against the record's serialized Kafka log key.</li>
 *   <li>{@code MESSAGE} - evaluate the filter against the record's serialized Kafka log message.</li>
 * </ul>
 */
public enum FilterDataSourceType {
    KEY, MESSAGE
}
