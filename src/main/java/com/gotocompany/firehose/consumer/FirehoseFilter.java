package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Bridges a {@link Filter} into the consumer loop and records metrics about filtered messages.
 *
 * <p>It delegates predicate evaluation to the configured {@link Filter} and, whenever a batch
 * contains messages that were filtered out, increments the filtered-message counter and the global
 * {@code FILTERED} message-scope metric through {@link FirehoseInstrumentation}.
 */
@AllArgsConstructor
public class FirehoseFilter {
    /** Underlying predicate that partitions a batch into valid and invalid messages. */
    private final Filter filter;
    /** Records counts of messages that were filtered out. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Applies the configured filter to a batch and records how many messages were filtered out.
     *
     * @param messages the messages read from Kafka for this batch
     * @return the partitioned result holding both valid and invalid (filtered-out) messages
     * @throws FilterException if the underlying filter fails to evaluate the batch
     */
    public FilteredMessages applyFilter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessage = filter.filter(messages);
        int filteredMessageCount = filteredMessage.sizeOfInvalidMessages();
        if (filteredMessageCount > 0) {
            firehoseInstrumentation.captureFilteredMessageCount(filteredMessageCount);
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.FILTERED, filteredMessageCount);
        }
        return filteredMessage;
    }
}
