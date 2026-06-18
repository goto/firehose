package com.gotocompany.firehose.message;


import com.gotocompany.firehose.exception.DefaultException;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.header.Headers;

import java.util.Base64;

/**
 * A class to hold a single protobuf message in binary format.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Message {
    /** Raw bytes of the record key. */
    private byte[] logKey;
    /** Raw bytes of the record value (the message payload). */
    private byte[] logMessage;
    /** Source Kafka topic. */
    private String topic;
    /** Source Kafka partition. */
    private int partition;
    /** Source Kafka offset. */
    private long offset;
    /** Kafka record headers, used for trace-context propagation. */
    private Headers headers;
    /** Producer/event timestamp of the record, in epoch milliseconds. */
    private long timestamp;
    /** Time the record was consumed by Firehose, in epoch milliseconds. */
    private long consumeTimestamp;
    /** Error attached to this message during processing, or {@code null} if none. */
    @Setter
    private ErrorInfo errorInfo;

    /**
     * Attaches a default {@link ErrorInfo} when none is already set.
     *
     * <p>Used before retry and DLQ handling so every failed message carries an error type.
     */
    public void setDefaultErrorIfNotPresent() {
        if (errorInfo == null) {
            errorInfo = new ErrorInfo(new DefaultException("DEFAULT"), ErrorType.DEFAULT_ERROR);
        }
    }

    /**
     * Instantiates a new Message.
     *
     * @param logKey     the log key
     * @param logMessage the log message
     * @param topic      the topic
     * @param partition  the partition
     * @param offset     the offset
     */
    public Message(byte[] logKey, byte[] logMessage, String topic, int partition, long offset) {
        this.logKey = logKey;
        this.logMessage = logMessage;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Instantiates a new Message without providing errorType.
     *
     * @param logKey
     * @param logMessage
     * @param topic
     * @param partition
     * @param offset
     * @param headers
     * @param timestamp
     * @param consumeTimestamp
     */
    public Message(byte[] logKey, byte[] logMessage, String topic, int partition, long offset, Headers headers, long timestamp, long consumeTimestamp) {
        this(logKey, logMessage, topic, partition, offset, headers, timestamp, consumeTimestamp, null);
    }

    /**
     * Creates a copy of an existing message with the given error attached.
     *
     * @param message   the message to copy the record fields from
     * @param errorInfo the error to attach to the new message
     */
    public Message(Message message, ErrorInfo errorInfo) {
        this(message.getLogKey(),
                message.getLogMessage(),
                message.getTopic(),
                message.getPartition(),
                message.getOffset(),
                message.getHeaders(),
                message.getTimestamp(),
                message.getConsumeTimestamp(),
                errorInfo);
    }

    /**
     * Gets serialized key.
     *
     * @return the serialized key
     */
    public String getSerializedKey() {
        return encodedSerializedStringFrom(logKey);
    }

    /**
     * Gets serialized message.
     *
     * @return the serialized message
     */
    public String getSerializedMessage() {
        return encodedSerializedStringFrom(logMessage);
    }

    /**
     * Base64-encodes the given bytes, returning an empty string for null or empty input.
     *
     * @param bytes the bytes to encode
     * @return the base64 encoding, or an empty string when there is nothing to encode
     */
    private static String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
