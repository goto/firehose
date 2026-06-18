package com.gotocompany.firehose.sink.influxdb.builder;

import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Point builder for influx metrics.
 */
public class PointBuilder {
    /** Error message used when the field index mapping is empty; at least one field value is required. */
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";
    /** Multiplier to convert seconds to milliseconds. */
    private static final long SECONDS_SCALED_TO_MILLI = 1000L;
    /** Divisor to convert nanoseconds to milliseconds. */
    private static final long MILLIS_SCALED_TO_NANOS = 1000000L;

    /** Builder for the point currently being assembled; reset on each {@link #buildPoint(DynamicMessage)}. */
    private Point.Builder pointBuilder;
    /** Mapping from proto field index to InfluxDB tag name; values may be nested {@link Properties}. */
    private Properties tagNameProtoIndexMapping;
    /** Mapping from proto field index to InfluxDB field name; values may be nested {@link Properties}. */
    private Properties fieldNameProtoIndexMapping;
    /** InfluxDB measurement name applied to every point. */
    private String measurementName;
    /** Proto field index of the event timestamp used as the point time. */
    private Integer timeStampIndex;

    /**
     * Instantiates a new Point builder.
     *
     * @param config the config
     */
    public PointBuilder(InfluxSinkConfig config) {
        tagNameProtoIndexMapping = config.getSinkInfluxTagNameProtoIndexMapping();
        fieldNameProtoIndexMapping = config.getSinkInfluxFieldNameProtoIndexMapping();
        this.timeStampIndex = config.getSinkInfluxProtoEventTimestampIndex();
        this.measurementName = config.getSinkInfluxMeasurementName();
    }

    /**
     * Builds an InfluxDB point from the given protobuf message.
     * <p>
     * Adds the configured tags and fields, then sets the point time from the configured event
     * timestamp field, in milliseconds.
     *
     * @param message the parsed protobuf message to convert
     * @return the assembled InfluxDB point
     * @throws InvalidProtocolBufferException if a timestamp field cannot be parsed
     */
    public Point buildPoint(DynamicMessage message) throws InvalidProtocolBufferException {
        this.pointBuilder = Point.measurement(measurementName);
        addTagsToPoint(message, tagNameProtoIndexMapping);
        addFieldsToPoint(message, fieldNameProtoIndexMapping);
        Timestamp timestamp = getTimestamp(message, timeStampIndex);
        pointBuilder.time(getMillisFromTimestamp(timestamp), TimeUnit.MILLISECONDS);
        return pointBuilder.build();
    }

    /**
     * Reads and parses the protobuf {@code Timestamp} at the given field index.
     *
     * @param message    the message to read from
     * @param fieldIndex the proto field index of the timestamp
     * @return the parsed timestamp
     * @throws InvalidProtocolBufferException if the timestamp bytes cannot be parsed
     */
    private Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    /**
     * Adds the configured tags to the point being built.
     * <p>
     * {@code Timestamp} and {@code Duration} fields are added as epoch milliseconds, string mappings
     * are added as plain tag values, and nested {@link Properties} mappings recurse into the
     * corresponding sub-message.
     *
     * @param message           the message supplying tag values
     * @param protoIndexMapping the proto index to tag-name mapping
     * @throws InvalidProtocolBufferException if a timestamp tag value cannot be parsed
     * @throws RuntimeException               if a mapping value is neither a string nor nested properties
     */
    private void addTagsToPoint(Message message, Properties protoIndexMapping) throws InvalidProtocolBufferException {
        for (Object protoFieldIndex : protoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object tagValue = getField(message, fieldIndex);
            Object tag = protoIndexMapping.get(protoFieldIndex);
            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                    || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                pointBuilder.tag((String) tag, getMillisFromTimestamp(getTimestamp(message, fieldIndex)).toString());
            } else if (tag instanceof String) {
                pointBuilder.tag((String) tag, tagValue.toString());
            } else if (tag instanceof Properties) {
                addTagsToPoint((Message) tagValue, (Properties) tag);
            } else {
                throw new RuntimeException("column can either be properties or string");
            }
        }
    }

    /**
     * Adds the configured fields to the point being built.
     * <p>
     * {@code Timestamp} and {@code Duration} fields are stored as epoch milliseconds, enum fields as
     * their names, other scalar fields by value, and nested {@link Properties} mappings recurse into
     * the corresponding sub-message.
     *
     * @param message           the message supplying field values
     * @param protoIndexMapping the proto index to field-name mapping
     * @throws InvalidProtocolBufferException if a timestamp field value cannot be parsed
     * @throws ConfigurationException         if the field mapping is empty
     * @throws RuntimeException               if a mapping value is neither a string nor nested properties
     */
    private void addFieldsToPoint(Message message, Properties protoIndexMapping) throws InvalidProtocolBufferException {
        if (protoIndexMapping.isEmpty()) {
            throw new ConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        Map<String, Object> fieldNameValueMap = new HashMap<>();
        for (Object protoFieldIndex : protoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object field = protoIndexMapping.get(protoFieldIndex);

            if (field instanceof String) {
                Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
                if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                        || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                    fieldNameValueMap.put((String) field, getMillisFromTimestamp(getTimestamp(message, fieldIndex)));
                } else if (fieldIsOfEnumType(fieldDescriptor)) {
                    fieldNameValueMap.put((String) field, getField(message, fieldIndex).toString());
                } else {
                    fieldNameValueMap.put((String) field, getField(message, fieldIndex));
                }
            } else if (field instanceof Properties) {
                addFieldsToPoint((Message) getField(message, fieldIndex), (Properties) field);
            } else {
                throw new RuntimeException("column can either be properties or string");
            }
        }
        pointBuilder.fields(fieldNameValueMap);
    }

    /**
     * Returns whether the field is a message of the given protobuf type.
     *
     * @param fieldDescriptor the descriptor of the field to test
     * @param typeDescriptor  the descriptor of the expected message type
     * @return {@code true} if the field is a message whose full name matches the expected type
     */
    private boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    /**
     * Returns whether the field is an enum.
     *
     * @param fieldDescriptor the descriptor of the field to test
     * @return {@code true} if the field type is an enum
     */
    private boolean fieldIsOfEnumType(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType().name().equals("ENUM");
    }

    /**
     * Returns the value of the field at the given proto index.
     *
     * @param message    the message to read from
     * @param protoIndex the proto field index
     * @return the field value
     */
    private Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    /**
     * Resolves the field descriptor for the given proto index.
     *
     * @param message    the message whose type is inspected
     * @param protoIndex the proto field index
     * @return the matching field descriptor
     */
    private Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    /**
     * Converts a protobuf timestamp into epoch milliseconds.
     *
     * @param timestamp the timestamp to convert
     * @return the timestamp value in milliseconds since the epoch
     */
    private Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }

}
