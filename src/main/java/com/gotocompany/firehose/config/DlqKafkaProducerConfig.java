package com.gotocompany.firehose.config;

/**
 * Owner configuration for the Kafka-backed DLQ writer, which republishes failed messages to a Kafka
 * topic.
 *
 * <p>It extends {@link DlqConfig} and adds the Kafka producer settings (acknowledgements, retries,
 * batching, buffering, serializers, brokers and the retry topic) used when the DLQ writer type is
 * Kafka. Each accessor maps to an environment variable via {@code @Key} and, where present, falls
 * back to its {@code @DefaultValue}.
 */
public interface DlqKafkaProducerConfig extends DlqConfig {

    /**
     * Returns the producer {@code acks} setting controlling write durability, set by
     * {@code DLQ_KAFKA_ACKS} and defaulting to {@code all}.
     *
     * @return the producer acknowledgement setting
     */
    @Key("DLQ_KAFKA_ACKS")
    @DefaultValue("all")
    String getDlqKafkaAcks();

    /**
     * Returns the number of times the producer retries a failed send, set by
     * {@code DLQ_KAFKA_RETRIES} and defaulting to {@code 2147483647} ({@code Integer.MAX_VALUE}).
     *
     * @return the producer retry count
     */
    @Key("DLQ_KAFKA_RETRIES")
    @DefaultValue("2147483647")
    String getDlqKafkaRetries();

    /**
     * Returns the producer {@code batch.size} in bytes, set by {@code DLQ_KAFKA_BATCH_SIZE} and
     * defaulting to {@code 16384}.
     *
     * @return the producer batch size in bytes
     */
    @Key("DLQ_KAFKA_BATCH_SIZE")
    @DefaultValue("16384")
    String getDlqKafkaBatchSize();

    /**
     * Returns the producer {@code linger.ms}, the time the producer waits to batch records, set by
     * {@code DLQ_KAFKA_LINGER_MS} and defaulting to {@code 0}.
     *
     * @return the producer linger time in milliseconds
     */
    @Key("DLQ_KAFKA_LINGER_MS")
    @DefaultValue("0")
    String getDlqKafkaLingerMs();

    /**
     * Returns the producer {@code buffer.memory} in bytes available for buffering unsent records,
     * set by {@code DLQ_KAFKA_BUFFER_MEMORY} and defaulting to {@code 33554432} (32 MiB).
     *
     * @return the producer buffer memory in bytes
     */
    @Key("DLQ_KAFKA_BUFFER_MEMORY")
    @DefaultValue("33554432")
    String getDlqKafkaBufferMemory();

    /**
     * Returns the producer key serializer class, set by {@code DLQ_KAFKA_KEY_SERIALIZER} and
     * defaulting to {@code org.apache.kafka.common.serialization.ByteArraySerializer}.
     *
     * @return the key serializer class name
     */
    @Key("DLQ_KAFKA_KEY_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaKeySerializer();

    /**
     * Returns the producer value serializer class, set by {@code DLQ_KAFKA_VALUE_SERIALIZER} and
     * defaulting to {@code org.apache.kafka.common.serialization.ByteArraySerializer}.
     *
     * @return the value serializer class name
     */
    @Key("DLQ_KAFKA_VALUE_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaValueSerializer();

    /**
     * Returns the comma-separated bootstrap brokers of the Kafka cluster the DLQ producer writes to,
     * set by {@code DLQ_KAFKA_BROKERS}.
     *
     * @return the DLQ Kafka bootstrap servers
     */
    @Key("DLQ_KAFKA_BROKERS")
    String getDlqKafkaBrokers();

    /**
     * Returns the Kafka topic failed messages are republished to, set by {@code DLQ_KAFKA_TOPIC} and
     * defaulting to {@code firehose-retry-topic}.
     *
     * @return the DLQ Kafka topic name
     */
    @Key("DLQ_KAFKA_TOPIC")
    @DefaultValue("firehose-retry-topic")
    String getDlqKafkaTopic();
}
