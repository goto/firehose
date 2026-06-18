package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * Sink decorator provides internal processing on the use provided sink type.
 */
public class SinkDecorator implements Sink {

    /** The wrapped sink that all operations are delegated to. */
    private final Sink sink;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */
    public SinkDecorator(Sink sink) {
        this.sink = sink;
    }

    /**
     * Delegates to the wrapped sink and returns the messages it failed to push.
     *
     * @param message the messages to push
     * @return the messages that could not be delivered, for further handling
     * @throws IOException           if the wrapped sink fails with an I/O error
     * @throws DeserializerException if the wrapped sink fails to deserialize a message
     */
    @Override
    public List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException {
        return sink.pushMessage(message);
    }

    /**
     * Closes the wrapped sink.
     *
     * @throws IOException if the wrapped sink fails to close
     */
    @Override
    public void close() throws IOException {
        sink.close();
    }

    /**
     * Delegates calculation of committable offsets to the wrapped sink.
     */
    @Override
    public void calculateCommittableOffsets() {
        sink.calculateCommittableOffsets();
    }

    /**
     * Reports whether the wrapped sink manages its own Kafka offsets.
     *
     * @return {@code true} if the wrapped sink manages offsets itself
     */
    @Override
    public boolean canManageOffsets() {
        return sink.canManageOffsets();
    }

    /**
     * Delegates offset tracking and release for the given messages to the wrapped sink.
     *
     * @param messageList the messages whose offsets to track and mark committable
     */
    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        sink.addOffsetsAndSetCommittable(messageList);
    }
}
