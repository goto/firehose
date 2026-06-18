package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import java.util.List;

/**
 * {@link Filter} that performs no filtering and marks every message as valid.
 *
 * <p>Selected when filtering is disabled ({@code FILTER_ENGINE=NO_OP}); it forwards all consumed
 * messages to the sink unchanged.
 */
public class NoOpFilter implements Filter {

    /**
     * Creates the no-op filter and logs that no filter is in effect.
     *
     * @param firehoseInstrumentation the instrumentation used to log the selection
     */
    public NoOpFilter(FirehoseInstrumentation firehoseInstrumentation) {
        firehoseInstrumentation.logInfo("No filter is selected");
    }

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return filtered messages.
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        messages.forEach(filteredMessages::addToValidMessages);
        return filteredMessages;
    }
}
