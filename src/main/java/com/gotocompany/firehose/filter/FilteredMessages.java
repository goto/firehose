package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds the result of applying a {@link Filter}: the messages that passed and those filtered out.
 *
 * <p>A filter adds each message to either the valid or the invalid list; the consumer then pushes
 * the valid messages to the sink and marks the invalid ones committable so they are skipped. Lombok
 * generates the getters together with {@code equals} and {@code hashCode}.
 */
@Getter
@EqualsAndHashCode
public class FilteredMessages {
    /** Messages that satisfied the filter and should be pushed to the sink. */
    private final List<Message> validMessages = new ArrayList<>();
    /** Messages that were filtered out and should be skipped (committed without sinking). */
    private final List<Message> invalidMessages = new ArrayList<>();

    /**
     * Adds a message to the valid (kept) set.
     *
     * @param message the message that passed the filter
     */
    public void addToValidMessages(Message message) {
        validMessages.add(message);
    }

    /**
     * Adds a message to the invalid (filtered-out) set.
     *
     * @param message the message that did not pass the filter
     */
    public void addToInvalidMessages(Message message) {
        invalidMessages.add(message);
    }

    /**
     * Returns the number of valid messages.
     *
     * @return the count of messages that passed the filter
     */
    public int sizeOfValidMessages() {
        return validMessages.size();
    }

    /**
     * Returns the number of invalid messages.
     *
     * @return the count of messages that were filtered out
     */
    public int sizeOfInvalidMessages() {
        return invalidMessages.size();
    }
}


