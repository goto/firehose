package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.exception.FirehoseConsumerFailedException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Firehose consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class FirehoseSyncConsumer implements FirehoseConsumer {

    /** Sink that every valid message in a batch is pushed to. */
    private final Sink sink;
    /** Opens and closes tracing spans around each processed batch. */
    private final SinkTracer tracer;
    /** Reads messages from Kafka and tracks the offsets eligible for commit. */
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    /** Applies the configured filter and records filtered-message metrics. */
    private final FirehoseFilter firehoseFilter;
    /** Emits logs and metrics for this consumer. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Processes a single batch end to end: read, trace, filter, push valid messages, then commit.
     *
     * <p>Invalid (filtered-out) messages are force-added to the committable offsets so they are not
     * re-consumed, while valid messages are pushed to the sink before their offsets are marked
     * committable. The total processing time is always captured as a duration metric, even when the
     * batch fails.
     *
     * @throws IOException if the sink fails while pushing the batch
     * @throws FirehoseConsumerFailedException if filtering fails with a {@link FilterException}
     */
    @Override
    public void process() throws IOException {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                sink.pushMessage(filteredMessages.getValidMessages());
                consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getValidMessages());
            }
            consumerAndOffsetManager.commit();
            firehoseInstrumentation.logInfo("Processed {} records in consumer", messages.size());
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    /**
     * Closes the sink, tracer, offset manager, and instrumentation held by this consumer.
     *
     * @throws IOException if any of the underlying resources fail to close
     */
    @Override
    public void close() throws IOException {
        sink.close();
        tracer.close();
        consumerAndOffsetManager.close();
        firehoseInstrumentation.close();
    }
}
