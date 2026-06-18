package com.gotocompany.firehose.sink.jdbc.field.message;

import com.gotocompany.firehose.sink.jdbc.field.JdbcField;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * {@link JdbcField} that serializes a protobuf {@link Message} into its JSON representation.
 * <p>
 * Uses a {@link JsonFormat.Printer} configured to omit insignificant whitespace, preserve proto field
 * names and include default values, so nested messages can be stored as JSON text. Acts as the default
 * strategy for message-typed fields.
 */
public class JdbcDefaultMessageField implements JdbcField {
    /** The field value to serialize; replaced with its JSON string during conversion. */
    private Object columnValue;
    /** Reusable JSON printer configured to preserve field names and include default values. */
    private JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();

    /**
     * Creates a message-field converter.
     *
     * @param columnValue the protobuf message value to serialize
     */
    public JdbcDefaultMessageField(Object columnValue) {
        this.columnValue = columnValue;
    }

    /**
     * Serializes the protobuf message into a JSON string.
     *
     * @return the JSON representation of the message
     * @throws RuntimeException if the message cannot be serialized, wrapping the underlying {@link InvalidProtocolBufferException}
     */
    @Override
    public Object getColumn() throws RuntimeException {
        try {
            columnValue = this.jsonPrinter.print((Message) columnValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return columnValue;
    }

    /**
     * Indicates whether the value is a protobuf message.
     *
     * @return {@code true} if the value is a protobuf {@link Message}
     */
    @Override
    public boolean canProcess() {
        return columnValue instanceof Message;
    }
}
