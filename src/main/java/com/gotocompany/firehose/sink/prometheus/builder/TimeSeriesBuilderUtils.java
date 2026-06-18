package com.gotocompany.firehose.sink.prometheus.builder;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Static helpers for extracting Prometheus metrics, labels and timestamps from a protobuf message.
 * <p>
 * Used by {@link TimeSeriesBuilder} to flatten the configured proto index mappings (recursing into
 * nested messages) into {@link PrometheusMetric} and {@link PrometheusLabel} sets, and to resolve the
 * metric timestamp.
 */
public class TimeSeriesBuilderUtils {

    /**
     * Extracts the configured metrics from the message.
     * <p>
     * String mappings produce a {@link PrometheusMetric} whose value is parsed as a double; nested
     * {@link Properties} mappings recurse into the corresponding sub-message.
     *
     * @param message                     the protobuf message to read values from
     * @param metricNameProtoIndexMapping the proto index to metric name mapping
     * @return the set of extracted metrics
     */
    public static Set<PrometheusMetric> getMetricsFromMessage(Message message, Properties metricNameProtoIndexMapping) {
        Set<PrometheusMetric> metrics = new HashSet<>();
        metricNameProtoIndexMapping.forEach((metricIndex, metricKey) -> {
            int fieldIndex = Integer.parseInt((String) metricIndex);
            Object metricValue = getField(message, fieldIndex);
            if (metricKey instanceof String) {
                metrics.add(new PrometheusMetric(metricKey.toString(), Double.parseDouble(metricValue.toString())));
            } else if (metricKey instanceof Properties) {
                metrics.addAll(getMetricsFromMessage((Message) metricValue, (Properties) metricKey));
            }
        });
        return metrics;
    }
    /**
     * @param message                    Protobuf message to read values from
     * @param labelNameProtoIndexMapping mapping of index to metric name
     * @param partition                  kafka partition to be set as default label
     * @return Set of Labels
     */

    public static Set<PrometheusLabel> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping, int partition) {
        Set<PrometheusLabel> labels = new HashSet<>();
        labels.add(new PrometheusLabel(PromSinkConstants.KAFKA_PARTITION, String.valueOf(partition)));
        labels.addAll(getLabelsFromMessage(message, labelNameProtoIndexMapping));
        return labels;
    }

    /**
     * Extracts the configured labels from the message.
     * <p>
     * String mappings produce a {@link PrometheusLabel}; nested {@link Properties} mappings recurse
     * into the corresponding sub-message.
     *
     * @param message                    the protobuf message to read values from
     * @param labelNameProtoIndexMapping the proto index to label name mapping
     * @return the set of extracted labels
     */
    private static Set<PrometheusLabel> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping) {
        Set<PrometheusLabel> labels = new HashSet<>();
        labelNameProtoIndexMapping.forEach((labelIndex, labelKey) -> {
            int fieldIndex = Integer.parseInt((String) labelIndex);
            Object labelValue = getField(message, fieldIndex);
            if (labelKey instanceof String) {
                labels.add(new PrometheusLabel(labelKey.toString(), getFieldValueString(message, fieldIndex)));
            } else if (labelKey instanceof Properties) {
                labels.addAll(getLabelsFromMessage((Message) labelValue, (Properties) labelKey));
            }
        });
        return labels;
    }

    /**
     * Returns the string value of a field, converting timestamp and duration fields to epoch millis.
     *
     * @param message    the message to read from
     * @param fieldIndex the proto field index
     * @return the field value as a string
     * @throws IllegalArgumentException if a timestamp field cannot be parsed
     */
    private static String getFieldValueString(Message message, int fieldIndex) {
        try {
            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                    || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                return getMillisFromTimestamp(getTimestamp(message, fieldIndex)).toString();
            } else {
                return getField(message, fieldIndex).toString();
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Timestamp that will be use as metric timestamp.
     * the timestamp from message or from current timestamp base on prometheus config
     *
     * @param message the protobuf message
     * @return the unix timestamp
     * @throws InvalidProtocolBufferException the exception on invalid protobuf
     */
    public static Long getMetricTimestamp(Message message, boolean isEventTimestampEnabled, int timestampIndex) throws InvalidProtocolBufferException {
        return (isEventTimestampEnabled) ? getMillisFromTimestamp(getTimestamp(message, timestampIndex)) : System.currentTimeMillis();
    }

    /**
     * Returns whether the field is a message of the given protobuf type.
     *
     * @param fieldDescriptor the descriptor of the field to test
     * @param typeDescriptor  the descriptor of the expected message type
     * @return {@code true} if the field is a message whose full name matches the expected type
     */
    private static boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    /**
     * Reads and parses the protobuf {@code Timestamp} at the given field index.
     *
     * @param message    the message to read from
     * @param fieldIndex the proto field index of the timestamp
     * @return the parsed timestamp
     * @throws InvalidProtocolBufferException if the timestamp bytes cannot be parsed
     */
    private static Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    /**
     * Returns the value of the field at the given proto index.
     *
     * @param message    the message to read from
     * @param protoIndex the proto field index
     * @return the field value
     */
    private static Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    /**
     * Resolves the field descriptor for the given proto index.
     *
     * @param message    the message whose type is inspected
     * @param protoIndex the proto field index
     * @return the matching field descriptor
     */
    private static Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    /**
     * Converts a protobuf timestamp into epoch milliseconds.
     *
     * @param timestamp the timestamp to convert
     * @return the timestamp value in milliseconds since the epoch
     */
    private static Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * PromSinkConstants.SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / PromSinkConstants.MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }
}
