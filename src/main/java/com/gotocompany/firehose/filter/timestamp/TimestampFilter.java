package com.gotocompany.firehose.filter.timestamp;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link Filter} that keeps only messages whose event timestamp falls within a configured window.
 *
 * <p>For each message the configured timestamp field (read from the key or value per
 * {@link FilterConfig#getFilterDataSource()}) is parsed from the protobuf payload using a Stencil
 * {@link Parser}. A message is valid when its timestamp is no older than
 * {@code FILTER_TIMESTAMP_PAST_WINDOW_SECONDS} and no further ahead than
 * {@code FILTER_TIMESTAMP_FUTURE_WINDOW_SECONDS} relative to now. The filter records detailed
 * counters (processed, valid, invalid, deserialization errors, and typed timestamp errors) and the
 * filter duration. Deserialization and processing errors are dropped (counted and filtered out) when
 * {@code FILTER_DROP_DESERIALIZATION_ERROR} is set, otherwise they raise a {@link FilterException}.
 */
public class TimestampFilter implements Filter {

    /** Common prefix for this filter's StatsD metrics. */
    private static final String METRIC_PREFIX = "firehose_timestamp_filter_";
    /** Metric counting messages processed by this filter. */
    private static final String MESSAGES_PROCESSED = METRIC_PREFIX + "messages_processed_total";
    /** Metric counting messages whose timestamp was within the window. */
    private static final String VALID_MESSAGES = METRIC_PREFIX + "valid_messages_total";
    /** Metric counting messages filtered out by this filter. */
    private static final String INVALID_MESSAGES = METRIC_PREFIX + "invalid_messages_total";
    /** Metric counting protobuf deserialization failures. */
    private static final String DESERIALIZATION_ERRORS = METRIC_PREFIX + "deserialization_errors_total";
    /** Metric counting messages whose timestamp field is missing from the schema. */
    private static final String UNKNOWN_FIELD_ERRORS = METRIC_PREFIX + "unknown_field_errors_total";
    /** Metric counting messages with missing, null, or out-of-window timestamps. */
    private static final String INVALID_TIMESTAMP_ERRORS = METRIC_PREFIX + "invalid_timestamp_errors_total";
    /** Metric counting timestamp fields of an unsupported type. */
    private static final String UNSUPPORTED_TYPE_ERRORS = METRIC_PREFIX + "unsupported_type_errors_total";
    /** Metric recording how long filtering a batch took, in milliseconds. */
    private static final String FILTER_DURATION_MS = METRIC_PREFIX + "duration_milliseconds";

    /** Filter configuration (timestamp field, windows, data source, schema). */
    private final FilterConfig filterConfig;
    /** Records logs, counters, and the duration metric. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Whether the key or the message value supplies the timestamp. */
    private final FilterDataSourceType filterDataSourceType;
    /** Name of the protobuf field that holds the event timestamp. */
    private final String timestampFieldName;
    /** Whether deserialization and processing errors are dropped rather than thrown. */
    private final boolean dropDeserializationError;
    /** Allowed lag behind now, in seconds; older timestamps are filtered out. */
    private final long pastWindowSeconds;
    /** Allowed lead ahead of now, in seconds; further-future timestamps are filtered out. */
    private final long futureWindowSeconds;
    /** Stencil parser used to deserialize the protobuf payload. */
    private final Parser parser;

    /**
     * Builds a timestamp filter from configuration and creates the protobuf parser.
     *
     * @param stencilClient           the Stencil client used to obtain the protobuf parser
     * @param filterConfig            the filter configuration (field name, windows, data source)
     * @param firehoseInstrumentation the instrumentation used for logging and metrics
     * @throws IllegalArgumentException if the proto schema class is not configured or the parser
     *                                  cannot be created
     */
    public TimestampFilter(StencilClient stencilClient, FilterConfig filterConfig,
            FirehoseInstrumentation firehoseInstrumentation) {
        this.filterConfig = filterConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.filterDataSourceType = filterConfig.getFilterDataSource();
        this.timestampFieldName = filterConfig.getFilterTimestampFieldName();
        this.dropDeserializationError = filterConfig.getFilterDropDeserializationError();
        this.pastWindowSeconds = filterConfig.getFilterTimestampPastWindowSeconds();
        this.futureWindowSeconds = filterConfig.getFilterTimestampFutureWindowSeconds();

        if (filterConfig.getFilterSchemaProtoClass() == null || filterConfig.getFilterSchemaProtoClass().isEmpty()) {
            throw new IllegalArgumentException(
                    "FILTER_SCHEMA_PROTO_CLASS configuration is required for timestamp filter");
        }

        this.parser = stencilClient.getParser(filterConfig.getFilterSchemaProtoClass());

        if (this.parser == null) {
            throw new IllegalArgumentException(
                    "Failed to create parser for " + filterConfig.getFilterSchemaProtoClass());
        }

        logConfiguration();
    }

    /**
     * Logs the resolved timestamp-filter configuration at startup.
     */
    private void logConfiguration() {
        firehoseInstrumentation.logInfo("\n\tFilter type: TIMESTAMP");
        firehoseInstrumentation.logInfo("\n\tFilter schema: {}", filterConfig.getFilterSchemaProtoClass());
        firehoseInstrumentation.logInfo("\n\tFilter timestamp field: {}", timestampFieldName);
        firehoseInstrumentation.logInfo("\n\tFilter past window (seconds): {}", pastWindowSeconds);
        firehoseInstrumentation.logInfo("\n\tFilter future window (seconds): {}", futureWindowSeconds);
        firehoseInstrumentation.logInfo("\n\tFilter drop deserialization error: {}", dropDeserializationError);
        firehoseInstrumentation.logInfo("\n\tFilter data source: {}", filterDataSourceType);
    }

    /**
     * Partitions messages by whether their event timestamp falls within the configured window.
     *
     * <p>Null or empty inputs yield an empty result. Each message is parsed and validated; processed,
     * valid, invalid, and duration metrics are recorded for the batch. A {@code null} message, or one
     * with empty data, is treated as invalid.
     *
     * @param messages the consumed records, each wrapping the raw bytes in a {@link Message}
     * @return the messages split into valid (in-window) and invalid lists
     * @throws FilterException if parsing or validation fails and errors are not dropped
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        if (messages == null) {
            firehoseInstrumentation.logWarn("Received null message list to filter");
            return new FilteredMessages();
        }

        if (messages.isEmpty()) {
            firehoseInstrumentation.logDebug("Received empty message list to filter");
            return new FilteredMessages();
        }

        long startTime = System.currentTimeMillis();
        int processedCount = 0;
        int validCount = 0;
        int invalidCount = 0;
        int deserializationErrorCount = 0;

        FilteredMessages filteredMessages = new FilteredMessages();

        for (Message message : messages) {
            processedCount++;
            try {
                if (message == null) {
                    firehoseInstrumentation.logWarn("Encountered null message. Skipping.");
                    invalidCount++;
                    filteredMessages.addToInvalidMessages(message);
                    continue;
                }

                byte[] data = (filterDataSourceType.equals(FilterDataSourceType.KEY)) ? message.getLogKey()
                        : message.getLogMessage();

                if (data == null || data.length == 0) {
                    firehoseInstrumentation.logWarn("Message has empty data. Skipping. Source: {}",
                            filterDataSourceType.equals(FilterDataSourceType.KEY) ? "KEY" : "MESSAGE");
                    invalidCount++;
                    filteredMessages.addToInvalidMessages(message);
                    continue;
                }

                DynamicMessage parsedMessage;
                try {
                    parsedMessage = parser.parse(data);
                } catch (InvalidProtocolBufferException e) {
                    deserializationErrorCount++;
                    firehoseInstrumentation.captureCount(DESERIALIZATION_ERRORS, 1L);
                    firehoseInstrumentation.logWarn("Failed to deserialize message: {}", e.getMessage());

                    if (dropDeserializationError) {
                        invalidCount++;
                        filteredMessages.addToInvalidMessages(message);
                    } else {
                        throw new FilterException("Failed to deserialize message", e);
                    }
                    continue;
                }

                if (isValidTimestamp(parsedMessage)) {
                    validCount++;
                    filteredMessages.addToValidMessages(message);
                } else {
                    invalidCount++;
                    filteredMessages.addToInvalidMessages(message);
                }
            } catch (Exception e) {
                if (dropDeserializationError) {
                    deserializationErrorCount++;
                    invalidCount++;
                    firehoseInstrumentation.captureCount(DESERIALIZATION_ERRORS, 1L);
                    firehoseInstrumentation.logWarn("Error processing message: {}", e.getMessage());
                    filteredMessages.addToInvalidMessages(message);
                } else {
                    throw new FilterException("Failed to process message", e);
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        firehoseInstrumentation.captureCount(MESSAGES_PROCESSED, (long) processedCount);
        firehoseInstrumentation.captureCount(VALID_MESSAGES, (long) validCount);
        firehoseInstrumentation.captureCount(INVALID_MESSAGES, (long) invalidCount);
        firehoseInstrumentation.captureValue(FILTER_DURATION_MS, (int) duration);

        firehoseInstrumentation.logInfo(
                "TimestampFilter processed {} messages in {}ms: {} valid, {} invalid, {} deserialization errors",
                processedCount, duration, validCount, invalidCount, deserializationErrorCount);

        return filteredMessages;
    }

    /**
     * Returns whether the message's timestamp field is present and within the configured window.
     *
     * <p>Emits typed {@code INVALID_TIMESTAMP_ERRORS} counters for missing, null, too-old, or
     * too-future timestamps, and counts an unknown-field error when the field is absent from the
     * schema.
     *
     * @param message the parsed protobuf message to inspect
     * @return {@code true} if the timestamp is within the past and future windows
     * @throws FilterException if the timestamp field is missing from the schema or cannot be read
     */
    private boolean isValidTimestamp(DynamicMessage message) throws FilterException {
        if (message == null) {
            firehoseInstrumentation.logWarn("Null message provided to timestamp validation");
            firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L);
            return false;
        }

        try {
            Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
                    .findFieldByName(timestampFieldName);

            if (fieldDescriptor == null) {
                firehoseInstrumentation.logWarn("Field '{}' not found in message type '{}'",
                        timestampFieldName, message.getDescriptorForType().getFullName());
                firehoseInstrumentation.captureCount(UNKNOWN_FIELD_ERRORS, 1L);
                throw new FilterException("Field " + timestampFieldName + " not found in message");
            }

            if (!message.hasField(fieldDescriptor)) {
                firehoseInstrumentation.logDebug("Message does not contain the timestamp field '{}'",
                        timestampFieldName);
                firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L,
                        "type=RECORD_MISSING_TIMESTAMP_FIELD");
                return false;
            }

            Object fieldValue = message.getField(fieldDescriptor);

            if (fieldValue == null) {
                firehoseInstrumentation.logDebug("Timestamp field '{}' has null value", timestampFieldName);
                firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L, "type=RECORD_NULL_TIMESTAMP_FIELD");
                return false;
            }

            long timestamp = extractTimestampValue(fieldValue);

            long currentTimeSeconds = Instant.now().getEpochSecond();
            long pastThreshold = currentTimeSeconds - pastWindowSeconds;
            long futureThreshold = currentTimeSeconds + futureWindowSeconds;

            if (firehoseInstrumentation.isDebugEnabled()) {
                firehoseInstrumentation.logDebug("Timestamp: {}, Current: {}, Past threshold: {}, Future threshold: {}",
                        timestamp, currentTimeSeconds, pastThreshold, futureThreshold);
            }

            boolean isInvalid = timestamp > futureThreshold || timestamp < pastThreshold;
            if (isInvalid) {
                if (timestamp < pastThreshold) {
                    firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L, "type=RECORD_TOO_OLD");
                    firehoseInstrumentation.logDebug(
                            "Message filtered out: timestamp {} is too old (past threshold: {})",
                            timestamp, pastThreshold);
                } else {
                    firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L, "type=RECORD_TOO_FUTURE");
                    firehoseInstrumentation.logDebug(
                            "Message filtered out: timestamp {} is too far in future (future threshold: {})",
                            timestamp, futureThreshold);
                }
            }

            return !isInvalid;
        } catch (Exception e) {
            firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L);
            throw new FilterException("Failed to validate timestamp: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts an epoch-seconds value from a timestamp field of various supported types.
     *
     * <p>Supports {@code Long} and {@code Integer} (epoch seconds), {@link Date} (converted from
     * milliseconds), numeric or ISO-8601 {@code String}, a {@code google.protobuf.Timestamp}
     * {@link DynamicMessage}, and generated protobuf {@code Timestamp} types.
     *
     * @param fieldValue the raw timestamp field value
     * @return the timestamp as epoch seconds
     * @throws FilterException if the value's type is unsupported or it cannot be parsed
     */
    private long extractTimestampValue(Object fieldValue) throws FilterException {
        if (fieldValue instanceof Long) {
            return (Long) fieldValue;
        } else if (fieldValue instanceof Integer) {
            return ((Integer) fieldValue).longValue();
        } else if (fieldValue instanceof Date) {
            return TimeUnit.MILLISECONDS.toSeconds(((Date) fieldValue).getTime());
        } else if (fieldValue instanceof String) {
            try {
                return Long.parseLong((String) fieldValue);
            } catch (NumberFormatException e) {
                try {
                    return Instant.parse((String) fieldValue).getEpochSecond();
                } catch (DateTimeException dateException) {
                    throw new FilterException("Could not parse String value as timestamp: " + fieldValue);
                }
            }
        } else if (fieldValue instanceof DynamicMessage) {
            DynamicMessage dynamicMsg = (DynamicMessage) fieldValue;
            String typeName = dynamicMsg.getDescriptorForType().getFullName();
            if ("google.protobuf.Timestamp".equals(typeName)) {
                return extractFromDynamicTimestamp(dynamicMsg);
            }
            firehoseInstrumentation.logDebug("Unrecognized DynamicMessage type: {}", typeName);
            firehoseInstrumentation.captureCount(UNSUPPORTED_TYPE_ERRORS, 1L);
            throw new FilterException("Unsupported DynamicMessage type: " + typeName);
        } else if (isProtobufTimestamp(fieldValue)) {
            return extractFromProtobufTimestamp(fieldValue);
        } else {
            firehoseInstrumentation.captureCount(UNSUPPORTED_TYPE_ERRORS, 1L);
            throw new FilterException("Unsupported timestamp field type: " + fieldValue.getClass().getName());
        }
    }

    /**
     * Heuristically determines whether an object is a generated protobuf {@code Timestamp}.
     *
     * @param obj the field value to test
     * @return {@code true} if the object appears to be a protobuf {@code Timestamp} and not a
     *         {@link DynamicMessage}
     */
    private boolean isProtobufTimestamp(Object obj) {
        if (obj instanceof DynamicMessage) {
            return false;
        }
        return obj.getClass().getName().endsWith("Timestamp")
                || obj.getClass().getName().equals("com.google.protobuf.Timestamp");
    }

    /**
     * Reads the seconds value from a generated protobuf {@code Timestamp} via reflection.
     *
     * @param protoTimestamp the protobuf {@code Timestamp} object
     * @return the timestamp's seconds component as epoch seconds
     * @throws FilterException if the seconds value cannot be read reflectively
     */
    private long extractFromProtobufTimestamp(Object protoTimestamp) throws FilterException {
        try {
            Method getSeconds = protoTimestamp.getClass().getMethod("getSeconds");
            Long seconds = (Long) getSeconds.invoke(protoTimestamp);

            try {
                Method getNanos = protoTimestamp.getClass().getMethod("getNanos");
                Integer nanos = (Integer) getNanos.invoke(protoTimestamp);
            } catch (Exception e) {
                firehoseInstrumentation.logDebug("Could not extract nanoseconds from timestamp: {}", e.getMessage());
            }

            return seconds;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new FilterException("Failed to extract seconds from Timestamp", e);
        }
    }

    /**
     * Reads the seconds value from a {@code google.protobuf.Timestamp} {@link DynamicMessage}.
     *
     * @param dynamicMsg the dynamic Timestamp message
     * @return the timestamp's seconds component as epoch seconds
     * @throws FilterException if the seconds field is missing, absent, or not a {@code Long}
     */
    private long extractFromDynamicTimestamp(DynamicMessage dynamicMsg) throws FilterException {
        try {
            Descriptors.FieldDescriptor secondsField = dynamicMsg.getDescriptorForType().findFieldByName("seconds");
            if (secondsField == null) {
                throw new FilterException("Field 'seconds' not found in google.protobuf.Timestamp");
            }

            if (!dynamicMsg.hasField(secondsField)) {
                throw new FilterException("Timestamp message does not contain 'seconds' field");
            }

            Object secondsValue = dynamicMsg.getField(secondsField);
            if (!(secondsValue instanceof Long)) {
                throw new FilterException("seconds field is not of type Long");
            }

            return (Long) secondsValue;
        } catch (Exception e) {
            if (!(e instanceof FilterException)) {
                e = new FilterException("Failed to extract seconds from DynamicMessage Timestamp", e);
            }
            throw (FilterException) e;
        }
    }
}
