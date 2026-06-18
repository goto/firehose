package com.gotocompany.firehose.config.enums;

/**
 * Selects which filter engine Firehose applies to incoming messages before they reach the sink.
 *
 * <p>The value is produced by
 * {@link com.gotocompany.firehose.config.converter.FilterEngineTypeConverter} and read by the
 * consumer factory to instantiate the matching filter. Records rejected by the chosen filter are
 * dropped and never forwarded to the sink.
 *
 * <ul>
 *   <li>{@code JEXL} - evaluate an Apache Commons JEXL expression against the message.</li>
 *   <li>{@code JSON} - evaluate a JSON-schema based filter against the message.</li>
 *   <li>{@code NO_OP} - apply no filtering, letting every message through; this is the default.</li>
 *   <li>{@code TIMESTAMP} - filter messages based on an event timestamp field.</li>
 * </ul>
 */
public enum FilterEngineType {
    JEXL, JSON, NO_OP, TIMESTAMP
}
