package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.exception.FirehoseConsumerFailedException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.SinkPool;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Asynchronous {@link FirehoseConsumer} that offloads sink writes to a pool of worker tasks so the
 * consumer thread can keep reading from Kafka while batches are pushed in the background.
 *
 * <p>On each {@link #process()} call it reads a batch through the {@link ConsumerAndOffsetManager},
 * opens trace spans, applies the {@link FirehoseFilter}, marks filtered-out messages committable,
 * then submits the valid messages to the {@link SinkPool} and registers the returned future with the
 * offset manager. Offsets for tasks that have finished since the previous call are marked committable
 * before a commit is attempted. Batch latency is recorded under
 * {@code SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS}. Instances are not thread-safe; the
 * launcher runs one consumer per thread.
 */
@AllArgsConstructor
public class FirehoseAsyncConsumer implements FirehoseConsumer {
    /** Pool of sinks that push batches concurrently on background threads. */
    private final SinkPool sinkPool;
    /** Opens and closes tracing spans around each processed batch. */
    private final SinkTracer tracer;
    /** Reads messages from Kafka and tracks the offsets eligible for commit. */
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    /** Applies the configured filter and records filtered-message metrics. */
    private final FirehoseFilter firehoseFilter;
    /** Emits logs and metrics for this consumer. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Reads, filters, and asynchronously pushes a single batch, then commits any finished work.
     *
     * <p>Valid messages are submitted to the sink pool as a task whose {@link Future} is registered
     * with the offset manager, while invalid messages are immediately marked committable. Offsets
     * belonging to tasks that have completed since the last invocation are then marked committable and
     * a commit is attempted.
     *
     * @throws FirehoseConsumerFailedException if filtering fails with a {@link FilterException}
     */
    @Override
    public void process() {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                List<Message> validMessages = filteredMessages.getValidMessages();
                Future<List<Message>> scheduledTask = scheduleTask(validMessages);
                consumerAndOffsetManager.addOffsets(scheduledTask, validMessages);
            }
            sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            consumerAndOffsetManager.commit();
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    /**
     * Submits the given messages to the sink pool, blocking until a worker slot becomes available.
     *
     * <p>When the pool's queue is full {@code submitTask} returns {@code null}; this method then
     * drains the already-finished sink tasks (marking their offsets committable) and retries until
     * the batch is accepted.
     *
     * @param messages the valid messages to push to the sink pool
     * @return a future representing the accepted, in-flight sink task
     */
    private Future<List<Message>> scheduleTask(List<Message> messages) {
        while (true) {
            Future<List<Message>> scheduledTask = sinkPool.submitTask(messages);
            if (scheduledTask == null) {
                firehoseInstrumentation.logInfo("The Queue is full");
                sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            } else {
                firehoseInstrumentation.logInfo("Adding sink task");
                return scheduledTask;
            }
        }
    }

    /**
     * Closes the sink pool, offset manager, tracer, and instrumentation held by this consumer.
     *
     * @throws IOException if any of the underlying resources fail to close
     */
    @Override
    public void close() throws IOException {
        sinkPool.close();
        consumerAndOffsetManager.close();
        tracer.close();
        firehoseInstrumentation.close();
    }
}
