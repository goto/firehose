package com.gotocompany.firehose.sink.jdbc.field.message;

import com.gotocompany.firehose.sink.jdbc.field.JdbcField;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link JdbcField} that converts a protobuf {@code Timestamp} message into an {@link Instant}.
 * <p>
 * Reads the {@code seconds} and {@code nanos} fields from the timestamp message and produces an
 * {@link Instant} so the value can be persisted as a SQL timestamp. Applies only when the value is a
 * {@link DynamicMessage} whose type is named {@code Timestamp}.
 */
public class JdbcTimestampField implements JdbcField {
    /** The raw field value, expected to be a protobuf {@code Timestamp} dynamic message. */
    private Object columnValue;

    /**
     * Creates a timestamp-field converter.
     *
     * @param columnValue the raw timestamp field value
     */
    public JdbcTimestampField(Object columnValue) {
        this.columnValue = columnValue;
    }

    /**
     * Converts the protobuf timestamp into an {@link Instant}.
     * <p>
     * Uses the first field as epoch seconds and the second as the nanosecond adjustment.
     *
     * @return the {@link Instant} represented by the timestamp message
     */
    @Override
    public Object getColumn() {
        List<Descriptors.FieldDescriptor> fieldDescriptors = ((DynamicMessage) columnValue).getDescriptorForType().getFields();
        ArrayList<Object> timeFields = new ArrayList<>();
        for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
            timeFields.add(((DynamicMessage) columnValue).getField(fieldDescriptor));
        }
        Instant instant = Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue());
        return instant;
    }

    /**
     * Indicates whether the value is a protobuf timestamp message.
     *
     * @return {@code true} if the value is a {@link DynamicMessage} of type {@code Timestamp}
     */
    @Override
    public boolean canProcess() {
        return columnValue instanceof DynamicMessage && ((DynamicMessage) columnValue).getDescriptorForType().getName().equals(Timestamp.class.getSimpleName());

    }
}
