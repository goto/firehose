package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ConsumerModeConverter;
import com.gotocompany.firehose.config.enums.KafkaConsumerMode;

/**
 * The interface for configurations required to instantiate a consumer.
 */
public interface KafkaConsumerConfig extends AppConfig {
    /**
     * Indicates whether offsets are committed asynchronously while running in async consumer mode,
     * set by {@code SOURCE_KAFKA_ASYNC_COMMIT_ENABLE} and defaulting to {@code true}.
     *
     * @return {@code true} if asynchronous offset commits are enabled
     */
    @Key("SOURCE_KAFKA_ASYNC_COMMIT_ENABLE")
    @DefaultValue("true")
    boolean isSourceKafkaAsyncCommitEnable();

    /**
     * Indicates whether only the partitions currently assigned to this consumer are committed,
     * set by {@code SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE} and defaulting to
     * {@code true}.
     *
     * @return {@code true} if commits are restricted to the currently assigned partitions
     */
    @Key("SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE")
    @DefaultValue("true")
    boolean isSourceKafkaCommitOnlyCurrentPartitionsEnable();

    /**
     * Returns the Kafka topic Firehose subscribes to for its input stream, set by
     * {@code SOURCE_KAFKA_TOPIC}.
     *
     * @return the source Kafka topic name
     */
    @Key("SOURCE_KAFKA_TOPIC")
    String getSourceKafkaTopic();

    /**
     * Returns the comma-separated list of Kafka bootstrap brokers to connect to, set by
     * {@code SOURCE_KAFKA_BROKERS}.
     *
     * @return the source Kafka bootstrap servers
     */
    @Key("SOURCE_KAFKA_BROKERS")
    String getSourceKafkaBrokers();

    /**
     * Returns the Kafka consumer group id under which Firehose tracks its committed offsets, set by
     * {@code SOURCE_KAFKA_CONSUMER_GROUP_ID}.
     *
     * @return the source Kafka consumer group id
     */
    @Key("SOURCE_KAFKA_CONSUMER_GROUP_ID")
    String getSourceKafkaConsumerGroupId();

    /**
     * Indicates whether the Kafka client commits offsets automatically (the {@code enable.auto.commit}
     * consumer property), set by {@code SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE} and
     * defaulting to {@code false} so that Firehose manages commits itself.
     *
     * @return {@code true} if Kafka auto-commit is enabled
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE")
    @DefaultValue("false")
    boolean isSourceKafkaConsumerConfigAutoCommitEnable();

    /**
     * Returns the Kafka {@code metadata.max.age.ms} value, the interval in milliseconds after which
     * cluster metadata is refreshed. Set by {@code SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS}
     * and defaulting to {@code 500}.
     *
     * @return the metadata max age in milliseconds
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMetadataMaxAgeMs();

    /**
     * Returns the Kafka {@code max.poll.records} value, the maximum number of records returned by a
     * single poll. Set by {@code SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS} and defaulting to
     * {@code 500}.
     *
     * @return the maximum records fetched per poll
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMaxPollRecords();

    /**
     * Returns the Kafka {@code session.timeout.ms} value, the time in milliseconds the broker waits
     * without a heartbeat before considering the consumer dead. Set by
     * {@code SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS} and defaulting to {@code 10000}.
     *
     * @return the consumer session timeout in milliseconds
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS")
    @DefaultValue("10000")
    int getSourceKafkaConsumerConfigSessionTimeoutMs();

    /**
     * Returns the maximum time in milliseconds a single Kafka poll call blocks waiting for records,
     * set by {@code SOURCE_KAFKA_POLL_TIMEOUT_MS} and defaulting to {@code 9223372036854775807}
     * ({@code Long.MAX_VALUE}, effectively unbounded).
     *
     * @return the poll timeout in milliseconds
     */
    @Key("SOURCE_KAFKA_POLL_TIMEOUT_MS")
    @DefaultValue("9223372036854775807")
    Long getSourceKafkaPollTimeoutMs();

    /**
     * Returns the consumer mode that decides whether Firehose runs synchronously or with an
     * asynchronous sink pool, set by {@code SOURCE_KAFKA_CONSUMER_MODE}, converted by
     * {@link com.gotocompany.firehose.config.converter.ConsumerModeConverter} and defaulting to
     * {@code SYNC}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.KafkaConsumerMode}
     */
    @Key("SOURCE_KAFKA_CONSUMER_MODE")
    @ConverterClass(ConsumerModeConverter.class)
    @DefaultValue("SYNC")
    KafkaConsumerMode getSourceKafkaConsumerMode();

    /**
     * Returns the minimum interval in milliseconds between manual offset commits, set by
     * {@code SOURCE_KAFKA_CONSUMER_CONFIG_MANUAL_COMMIT_MIN_INTERVAL_MS} and defaulting to
     * {@code -1}, which disables the minimum-interval throttling.
     *
     * @return the minimum manual-commit interval in milliseconds, or {@code -1} when disabled
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_MANUAL_COMMIT_MIN_INTERVAL_MS")
    @DefaultValue("-1")
    long getSourceKafkaConsumerManualCommitMinIntervalMs();

    /**
     * Returns the Kafka {@code partition.assignment.strategy} class name used to assign partitions
     * across consumers in the group, set by
     * {@code SOURCE_KAFKA_CONSUMER_CONFIG_PARTITION_ASSIGNMENT_STRATEGY} and defaulting to
     * {@code org.apache.kafka.clients.consumer.CooperativeStickyAssignor}.
     *
     * @return the fully-qualified partition assignment strategy class name
     */
    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_PARTITION_ASSIGNMENT_STRATEGY")
    @DefaultValue("org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
    String getSourceKafkaConsumerConfigPartitionAssignmentStrategy();

}
