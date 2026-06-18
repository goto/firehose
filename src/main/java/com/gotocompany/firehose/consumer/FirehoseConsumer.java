package com.gotocompany.firehose.consumer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Lifecycle contract for a Firehose consumer that pulls a batch of messages from the source and
 * delivers it to the configured sink.
 *
 * <p>Each consumer thread created by {@link com.gotocompany.firehose.launch.Task} builds an instance
 * through {@link FirehoseConsumerFactory} and repeatedly invokes {@link #process()} in a tight loop
 * until the thread is interrupted during shutdown, after which {@link #close()} (inherited from
 * {@link Closeable}) releases the Kafka consumer, sink, tracer, and metrics resources.
 * Implementations differ in how they push to the sink: {@link FirehoseSyncConsumer} pushes on the
 * calling thread while {@link FirehoseAsyncConsumer} dispatches the work to a pool of asynchronous
 * sink tasks.
 */
public interface FirehoseConsumer extends Closeable {

    /**
     * Reads the next batch of messages from the source, applies filtering, pushes the valid messages
     * to the sink, and advances the committable Kafka offsets.
     *
     * @throws IOException if reading from the source, pushing to the sink, or committing fails
     */
    void process() throws IOException;
}
