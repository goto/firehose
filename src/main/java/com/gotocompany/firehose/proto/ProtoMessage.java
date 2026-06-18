package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

import java.lang.reflect.Method;

/**
 * Reflectively reads a field from a serialized protobuf message by its field number.
 *
 * <p>Constructed with a fully-qualified proto class name, it resolves that class's static
 * {@code parseFrom(byte[])} method once and reuses it to parse each {@link Message}'s payload,
 * exposing a single field via {@link #get(Message, int)}. Configuration problems (a missing or
 * invalid proto class) surface as {@link ConfigurationException}; parse failures surface as
 * {@link DeserializerException}.
 */
public class ProtoMessage {
    /** Error message used when the configured proto class cannot be found. */
    public static final String CLASS_NAME_NOT_FOUND = "proto class provided in the configuration was not found";
    /** Error message used when the configured proto class has no {@code parseFrom} method. */
    public static final String INVALID_PROTOCOL_CLASS_MESSAGE = "Invalid proto class provided in the configuration";
    /** Error message used when a message payload cannot be deserialized. */
    public static final String DESERIALIZE_ERROR_MESSAGE = "Esb message could not be parsed";
    /** Cached reflective handle to the proto class's static {@code parseFrom(byte[])} method. */
    private Method messageParser;

    /**
     * Resolves the {@code parseFrom} method for the given proto class.
     *
     * @param protoClassName the fully-qualified name of the generated protobuf class
     * @throws ConfigurationException if the class is not found or has no {@code parseFrom} method
     */
    public ProtoMessage(String protoClassName) {
        this.messageParser = parserMethod(protoClassName);
    }

    /**
     * Parses the message and returns the value of the field with the given proto field number.
     *
     * @param message    the Firehose message whose payload is parsed
     * @param protoIndex the protobuf field number to read
     * @return the value of the requested field
     * @throws DeserializerException if the payload cannot be parsed
     */
    public Object get(Message message, int protoIndex) throws DeserializerException {
        GeneratedMessageV3 protoMsg;
        protoMsg = (GeneratedMessageV3) parseProtobuf(message);
        Descriptors.FieldDescriptor fieldDescriptor = protoMsg.getDescriptorForType().findFieldByNumber(protoIndex);
        return protoMsg.getField(fieldDescriptor);
    }

    /**
     * Parses the message payload into a protobuf object using the resolved {@code parseFrom} method.
     *
     * @param message the Firehose message whose payload is parsed
     * @return the parsed protobuf object
     * @throws DeserializerException if the payload cannot be parsed
     */
    public Object parseProtobuf(Message message) throws DeserializerException {
        try {
            return messageParser.invoke(null, message.getLogMessage());
        } catch (ReflectiveOperationException e) {
            throw new DeserializerException(DESERIALIZE_ERROR_MESSAGE, e);
        }
    }

    /**
     * Resolves the static {@code parseFrom(byte[])} method of the named proto class.
     *
     * @param protoClassName the fully-qualified name of the generated protobuf class
     * @return the reflective handle to the {@code parseFrom} method
     * @throws ConfigurationException if the class is not found or lacks a {@code parseFrom} method
     */
    private Method parserMethod(String protoClassName) {
        Class<com.google.protobuf.Message> builderClass;
        try {
            builderClass = (Class<com.google.protobuf.Message>) Class.forName(protoClassName);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(CLASS_NAME_NOT_FOUND, e);
        }
        try {
            return builderClass.getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new ConfigurationException(INVALID_PROTOCOL_CLASS_MESSAGE, e);
        }
    }
}
