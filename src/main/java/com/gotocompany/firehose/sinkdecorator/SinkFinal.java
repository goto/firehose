package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * Final decorator in the sink chain that accounts for messages no downstream sink could deliver.
 *
 * <p>After the inner decorators (fail handler, retry, DLQ) have run, any messages still returned as
 * failed are considered unrecoverable and are dropped. This decorator logs how many were ignored and
 * records them under the global {@code IGNORED} message scope so the data loss is observable.
 */
public class SinkFinal extends SinkDecorator {
    /** Records logs and the ignored-message metric for undeliverable messages. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */

    public SinkFinal(Sink sink, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Pushes messages through the chain and records any that remain undelivered as ignored.
     *
     * @param inputMessages the messages to push
     * @return the messages that could not be delivered, which are also counted as ignored
     * @throws IOException           if the wrapped sink fails with an I/O error
     * @throws DeserializerException if the wrapped sink fails to deserialize a message
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.size() > 0) {
            firehoseInstrumentation.logInfo("Ignoring messages {}", failedMessages.size());
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, failedMessages.size());
        }
        return failedMessages;
    }
}
