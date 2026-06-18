package com.gotocompany.firehose.exception;

/**
 * Unchecked exception that wraps a fatal error which aborts the Firehose consumer loop.
 *
 * <p>It is raised by the consumers (for example
 * {@link com.gotocompany.firehose.consumer.FirehoseSyncConsumer} and
 * {@link com.gotocompany.firehose.consumer.FirehoseAsyncConsumer}) when processing a batch of Kafka
 * messages fails in a way that cannot be retried or recovered from. The original cause is preserved
 * so the launcher can log it and shut the application down cleanly.
 */
public class FirehoseConsumerFailedException extends RuntimeException {
    /**
     * Wraps the underlying failure that caused the consumer to abort.
     *
     * @param th the root cause of the consumer failure
     */
    public FirehoseConsumerFailedException(Throwable th) {
        super(th);
    }
}
