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

public class TimestampFilter implements Filter {

    private static final String METRIC_PREFIX = "firehose_timestamp_filter_";
    private static final String MESSAGES_PROCESSED = METRIC_PREFIX + "messages_processed_total";
    private static final String VALID_MESSAGES = METRIC_PREFIX + "valid_messages_total";
    private static final String INVALID_MESSAGES = METRIC_PREFIX + "invalid_messages_total";
    private static final String DESERIALIZATION_ERRORS = METRIC_PREFIX + "deserialization_errors_total";
    private static final String UNKNOWN_FIELD_ERRORS = METRIC_PREFIX + "unknown_field_errors_total";
    private static final String INVALID_TIMESTAMP_ERRORS = METRIC_PREFIX + "invalid_timestamp_errors_total";
    private static final String UNSUPPORTED_TYPE_ERRORS = METRIC_PREFIX + "unsupported_type_errors_total";
    private static final String FILTER_DURATION_MS = METRIC_PREFIX + "duration_milliseconds";

    private final FilterConfig filterConfig;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final FilterDataSourceType filterDataSourceType;
    private final String timestampFieldName;
    private final boolean dropDeserializationError;
    private final long pastWindowSeconds;
    private final long futureWindowSeconds;
    private final Parser parser;

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

    private void logConfiguration() {
        firehoseInstrumentation.logInfo("\n\tFilter type: TIMESTAMP");
        firehoseInstrumentation.logInfo("\n\tFilter schema: {}", filterConfig.getFilterSchemaProtoClass());
        firehoseInstrumentation.logInfo("\n\tFilter timestamp field: {}", timestampFieldName);
        firehoseInstrumentation.logInfo("\n\tFilter past window (seconds): {}", pastWindowSeconds);
        firehoseInstrumentation.logInfo("\n\tFilter future window (seconds): {}", futureWindowSeconds);
        firehoseInstrumentation.logInfo("\n\tFilter drop deserialization error: {}", dropDeserializationError);
        firehoseInstrumentation.logInfo("\n\tFilter data source: {}", filterDataSourceType);
    }

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
                firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L);
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

            boolean isValid = timestamp >= pastThreshold && timestamp <= futureThreshold;

            if (!isValid) {
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

            return isValid;
        } catch (FilterException e) {
            throw e;
        } catch (Exception e) {
            firehoseInstrumentation.captureCount(INVALID_TIMESTAMP_ERRORS, 1L);
            throw new FilterException("Failed to validate timestamp: " + e.getMessage(), e);
        }
    }

    private long extractTimestampValue(Object fieldValue) throws FilterException {
        try {
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
        } catch (Exception e) {
            if (!(e instanceof FilterException)) {
                e = new FilterException("Error extracting timestamp from value: " + fieldValue, e);
            }
            throw (FilterException) e;
        }
    }

    private boolean isProtobufTimestamp(Object obj) {
        if (obj instanceof DynamicMessage) {
            return false;
        }
        return obj.getClass().getName().endsWith("Timestamp")
                || obj.getClass().getName().equals("com.google.protobuf.Timestamp");
    }

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
