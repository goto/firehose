package com.gotocompany.firehose.config.enums;

/**
 * Selects the strategy Firehose uses to consume Kafka records and drive the configured sink.
 *
 * <p>The mode is resolved from the {@code SOURCE_KAFKA_CONSUMER_MODE} setting (see
 * {@link com.gotocompany.firehose.config.KafkaConsumerConfig#getSourceKafkaConsumerMode()})
 * and decides which consumer implementation the consumer factory builds at startup.
 */
public enum KafkaConsumerMode {
    /**
     * Processes batches through a pool of sinks running on several worker threads, allowing
     * multiple batches to be in flight before their offsets are committed. This mode builds a
     * {@code FirehoseAsyncConsumer} backed by a sink pool sized by the sink pool configuration.
     */
    ASYNC,
    /**
     * Processes a single batch at a time on one sink, pushing each batch fully before the next is
     * polled. This mode builds a {@code FirehoseSyncConsumer} and is the default consumer mode.
     */
    SYNC
}
