package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Timestamp;

/**
 * TimestampMetadataProtoMessage contains proto schema proto message of Timestamp. The schema equals to {@link com.google.protobuf.Timestamp Timestamp} message.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of {@link com.google.protobuf.Timestamp Timestamp} proto message.
 *
 * message Timestamp {
 *     int64 seconds = 1
 *     int42 nanos = 2;
 * }
 *
 */
public class TimestampMetadataProtoMessage {
    /** Protobuf message type name, matching the well-known {@code Timestamp}. */
    private static final String TYPE_NAME = "Timestamp";
    /** Field name holding the whole-seconds component. */
    public static final String SECONDS_FIELD_NAME = "seconds";
    /** Field name holding the nanosecond component. */
    public static final String NANOS_FIELD_NAME = "nanos";
    /** Protobuf field number for {@link #SECONDS_FIELD_NAME}. */
    public static final int SECONDS_FIELD_NUMBER = 1;
    /** Protobuf field number for {@link #NANOS_FIELD_NAME}. */
    public static final int NANOS_FIELD_NUMBER = 2;

    /**
     * Builds the protobuf {@link com.github.os72.protobuf.dynamic.MessageDefinition} for the
     * {@code Timestamp} message, declaring its seconds and nanos fields.
     *
     * @return the message definition for the timestamp message
     */
    public static MessageDefinition createMessageDefinition() {
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", SECONDS_FIELD_NAME, SECONDS_FIELD_NUMBER)
                .addField("optional", "int32", NANOS_FIELD_NAME, NANOS_FIELD_NUMBER)
                .build();
    }

    /**
     * Returns the protobuf type name of the timestamp message.
     *
     * @return the type name {@code Timestamp}
     */
    public static String getTypeName() {
        return TYPE_NAME;
    }

    /**
     * Creates a builder for the well-known protobuf {@link com.google.protobuf.Timestamp}.
     *
     * @return a new {@link com.google.protobuf.Timestamp.Builder}
     */
    public static Timestamp.Builder newBuilder() {
        return Timestamp.newBuilder();
    }
}
