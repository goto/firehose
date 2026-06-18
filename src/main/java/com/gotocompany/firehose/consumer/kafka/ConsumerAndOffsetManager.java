package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * This class has APIs to read from kafka and also provide offset management.
 * There are 2 use cases for this class.
 * 1. FirehoseConsumer:
 * consumerOffsetManager.readMessagesFromKafka(); // Read messages from kafka.
 * consumerOffsetManager.addOffsetsAndSetCommittable(messages); // add offsets for messages.
 * consumerOffsetManager.commit(); // commit all committable offsets for all partitions.
 * <p>
 * 2. FirehoseAsyncConsumer:
 * consumerOffsetManager.readMessagesFromKafka();
 * consumerOffsetManager.addOffsets(key, messages);
 * consumerOffsetManager.setCommittable(key);
 * consumerOffsetManager.commit();
 * <p>
 * <p>
 * OffsetManager is shared between consumer and the sink.
 * So the offsets added there will be available here to commit.
 * <p>
 * consumerOffsetManager.commit() calls the sink method to calculate committable offsets.
 * then it fetches the offsets from offsetManager.getCommittableOffsets() and uses kafka api to commit.
 */
public class ConsumerAndOffsetManager implements AutoCloseable {
    /** Shared registry of offsets eligible for commit. */
    private final OffsetManager offsetManager;
    /** Sinks sharing this offset manager; the first is sampled for offset-management capability. */
    private final List<Sink> sinks;
    /** Wrapper used to read from and commit to Kafka. */
    private final FirehoseKafkaConsumer firehoseKafkaConsumer;
    /** Resolved Kafka consumer configuration controlling commit behaviour. */
    private final KafkaConsumerConfig kafkaConsumerConfig;
    /** Instrumentation for lifecycle logging. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Cached result of the sink's offset-management capability, sampled at construction. */
    private final boolean canSinkManageOffsets;
    /** Epoch millis of the last commit, used to throttle how often commits are issued. */
    private long lastCommitTimeStamp = 0;

    /**
     * Creates the coordinator and samples whether the sink manages its own offsets.
     *
     * @param sinks                   the sink instances sharing this offset manager
     * @param offsetManager           the shared offset registry
     * @param firehoseKafkaConsumer   the wrapper used to read from and commit to Kafka
     * @param kafkaConsumerConfig     the resolved Kafka consumer configuration
     * @param firehoseInstrumentation the instrumentation used for logging
     */
    public ConsumerAndOffsetManager(
            List<Sink> sinks,
            OffsetManager offsetManager,
            FirehoseKafkaConsumer firehoseKafkaConsumer,
            KafkaConsumerConfig kafkaConsumerConfig,
            FirehoseInstrumentation firehoseInstrumentation) {
        this.sinks = sinks;
        this.offsetManager = offsetManager;
        this.firehoseKafkaConsumer = firehoseKafkaConsumer;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.canSinkManageOffsets = sinks.get(0).canManageOffsets();
    }

    /**
     * Tracks the offsets of the given messages under a task key, unless the sink manages offsets.
     *
     * @param key      the key identifying the in-flight batch or task
     * @param messages the messages whose offsets to track
     */
    public void addOffsets(Object key, List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetToBatch(key, messages);
        }
    }

    /**
     * Marks the offsets registered under the given key committable, unless the sink manages offsets.
     *
     * @param key the key whose offsets can now be committed
     */
    public void setCommittable(Object key) {
        if (!canSinkManageOffsets) {
            offsetManager.setCommittable(key);
        }
    }

    /**
     * Tracks and immediately releases the offsets of the given messages, unless the sink manages
     * offsets.
     *
     * @param messages the messages whose offsets are ready to commit
     */
    public void addOffsetsAndSetCommittable(List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetsAndSetCommittable(messages);
        }
    }

    /**
     * Force-Update the offsets into offset manager regardless of sink managing the offsets.
     *
     * @param messages list of messages set to be committable
     */
    public void forceAddOffsetsAndSetCommittable(List<Message> messages) {
        offsetManager.addOffsetsAndSetCommittable(messages);
    }

    /**
     * Reads the next batch of messages from Kafka.
     *
     * @return the messages polled in this cycle, which may be empty
     */
    public List<Message> readMessages() {
        return firehoseKafkaConsumer.readMessages();
    }

    /**
     * Commits offsets to Kafka when the minimum commit interval has elapsed.
     *
     * <p>When {@code SOURCE_KAFKA_CONSUMER_COMMIT_ONLY_CURRENT_PARTITIONS} is enabled the sinks are
     * first asked to calculate their committable offsets and only those are committed; otherwise the
     * consumer's full current position is committed. Does nothing if called again within
     * {@code SOURCE_KAFKA_CONSUMER_MANUAL_COMMIT_MIN_INTERVAL_MS} of the previous commit.
     */
    public void commit() {
        long currentTimeStamp = System.currentTimeMillis();
        if (currentTimeStamp - lastCommitTimeStamp > kafkaConsumerConfig.getSourceKafkaConsumerManualCommitMinIntervalMs()) {
            if (kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable()) {
                sinks.forEach(Sink::calculateCommittableOffsets);
                firehoseKafkaConsumer.commit(offsetManager.getCommittableOffset());
            } else {
                firehoseKafkaConsumer.commit();
            }
            lastCommitTimeStamp = currentTimeStamp;
        }
    }

    /**
     * Closes the underlying Kafka consumer if it is present.
     *
     * @throws IOException if closing the Kafka consumer fails
     */
    @Override
    public void close() throws IOException {
        if (firehoseKafkaConsumer != null) {
            firehoseInstrumentation.logInfo("closing consumer");
            firehoseKafkaConsumer.close();
        }
    }

}
