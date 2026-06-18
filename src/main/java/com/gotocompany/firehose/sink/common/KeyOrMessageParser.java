package com.gotocompany.firehose.sink.common;


import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.message.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.stencil.Parser;
import lombok.AllArgsConstructor;

import java.io.IOException;

/**
 * Parser for Key or message.
 */
@AllArgsConstructor
public class KeyOrMessageParser {

    /** Stencil parser used to decode the selected payload. */
    private Parser protoParser;
    /** Application configuration providing the record parser mode. */
    private AppConfig appConfig;

    /**
     * Parse dynamic message.
     *
     * @param message the message
     * @return the dynamic message
     * @throws IOException when invalid message is encountered
     */
    public DynamicMessage parse(Message message) throws IOException {
        if (appConfig.getKafkaRecordParserMode().equals("key")) {
            return protoParse(message.getLogKey());
        }
        return protoParse(message.getLogMessage());
    }

    /**
     * Parses raw bytes into a dynamic protobuf message.
     *
     * @param data the protobuf-encoded bytes
     * @return the parsed dynamic message
     * @throws IOException if the bytes are not valid protobuf
     */
    private DynamicMessage protoParse(byte[] data) throws IOException {
        try {
            return protoParser.parse(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }
}
