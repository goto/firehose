package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.message.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.gotocompany.firehose.metrics.Metrics.FAILURE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.SUCCESS_TAG;

/**
 * A class responsible for consuming and committing kafka records.
 */
public class FirehoseKafkaConsumer implements AutoCloseable {

    /** Underlying Kafka consumer of byte-array keys and values. */
    private final Consumer<byte[], byte[]> kafkaConsumer;
    /** Resolved Kafka consumer configuration (poll timeout, async-commit flag, and so on). */
    private final KafkaConsumerConfig consumerConfig;
    /** Instrumentation for pull and commit logging and metrics. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Cache of the highest committed offset per partition, used to skip redundant commits. */
    private final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new ConcurrentHashMap<>();

    /**
     * A Constructor.
     *
     * @param kafkaConsumer           {@see KafkaConsumer}
     * @param config                  Consumer configuration.
     * @param firehoseInstrumentation Contain logging and metrics collection
     */
    public FirehoseKafkaConsumer(Consumer<byte[], byte[]> kafkaConsumer, KafkaConsumerConfig config, FirehoseInstrumentation firehoseInstrumentation) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     */
    public List<Message> readMessages() {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(consumerConfig.getSourceKafkaPollTimeoutMs()));
        firehoseInstrumentation.logInfo("Pulled {} messages", records.count());
        firehoseInstrumentation.capturePulledMessageHistogram(records.count());
        firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.CONSUMER, records.count());
        List<Message> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp(), System.currentTimeMillis()));
            firehoseInstrumentation.logDebug("Pulled record: {}", record);
        }
        return messages;
    }

    /**
     * Closes the underlying Kafka consumer, recording any failure as a non-fatal error.
     */
    public void close() {
        try {
            firehoseInstrumentation.logInfo("Consumer is closing");
            this.kafkaConsumer.close();
        } catch (Exception e) {
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "Exception while closing consumer");
        }
    }

    /**
     * Commits the current consumer position for all assigned partitions.
     *
     * <p>Uses an asynchronous commit when {@code SOURCE_KAFKA_ASYNC_COMMIT_ENABLE} is set, recording
     * success or failure under {@code SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL}; otherwise it commits
     * synchronously.
     */
    public void commit() {
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            kafkaConsumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    firehoseInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
                } else {
                    firehoseInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
                }
            });
        } else {
            kafkaConsumer.commitSync();
        }
    }

    /**
     * Commits the given per-partition offsets, skipping any that do not advance the last commit.
     *
     * <p>Only offsets greater than the cached committed offset for their partition are sent to Kafka,
     * which avoids redundant commits. The commit is asynchronous or synchronous depending on
     * {@code SOURCE_KAFKA_ASYNC_COMMIT_ENABLE}, and the cache of committed offsets is updated on
     * success.
     *
     * @param offsets the candidate offsets to commit, keyed by topic-partition
     */
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> latestOffsets =
                offsets.entrySet()
                        .stream()
                        .filter(metadataEntry -> !committedOffsets.containsKey(metadataEntry.getKey())
                                || metadataEntry.getValue().offset() > committedOffsets.get(metadataEntry.getKey()).offset())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (latestOffsets.isEmpty()) {
            return;
        }
        latestOffsets.forEach((k, v) ->
                firehoseInstrumentation.logInfo("Committing Offsets " + k.topic() + ":" + k.partition() + "=>" + v.offset()));
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync(latestOffsets);
        } else {
            kafkaConsumer.commitSync(latestOffsets);
        }
        committedOffsets.putAll(latestOffsets);
    }

    /**
     * Performs an asynchronous commit of the given offsets, reporting the result via
     * {@link #onComplete}.
     *
     * @param offsets the offsets to commit, keyed by topic-partition
     */
    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitAsync(offsets, this::onComplete);
    }

    /**
     * Callback that records whether an asynchronous commit succeeded or failed.
     *
     * @param offsets   the offsets that were the subject of the commit
     * @param exception the failure if the commit did not succeed, otherwise {@code null}
     */
    private void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            firehoseInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
        } else {
            firehoseInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
        }
    }
}
