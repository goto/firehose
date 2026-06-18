package com.gotocompany.firehose.message;

import com.gotocompany.depot.common.Tuple;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapts Firehose {@link Message} instances to depot's message model.
 *
 * <p>Sinks built on the depot library consume {@code com.gotocompany.depot.message.Message}; this
 * helper converts a batch of Firehose messages into that form, mapping the topic, partition, offset,
 * headers, event timestamp, and consume time onto depot metadata tuples.
 */
public class FirehoseMessageUtils {

    /**
     * Converts Firehose messages into depot messages for depot-based sinks.
     *
     * @param messages the Firehose messages to convert
     * @return the equivalent depot messages, preserving key, value, and metadata
     */
    public static List<com.gotocompany.depot.message.Message> convertToDepotMessage(List<Message> messages) {
        return messages.stream().map(message ->
                        new com.gotocompany.depot.message.Message(
                                message.getLogKey(),
                                message.getLogMessage(),
                                new Tuple<>("message_topic", message.getTopic()),
                                new Tuple<>("message_partition", message.getPartition()),
                                new Tuple<>("message_offset", message.getOffset()),
                                new Tuple<>("message_headers", message.getHeaders()),
                                new Tuple<>("message_timestamp", message.getTimestamp()),
                                new Tuple<>("load_time", message.getConsumeTimestamp())))
                .collect(Collectors.toList());
    }
}
