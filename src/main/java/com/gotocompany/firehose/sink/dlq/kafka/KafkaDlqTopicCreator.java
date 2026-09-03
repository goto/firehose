package com.gotocompany.firehose.sink.dlq.kafka;

import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.utils.KafkaProducerTypesMetadata;
import com.gotocompany.firehose.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Creates a Kafka DLQ topic when it is missing, using dagstream-compatible defaults.
 */
public class KafkaDlqTopicCreator {

    static final int DEFAULT_PARTITIONS = 3;
    static final short DEFAULT_REPLICATION_FACTOR = 3;
    static final long MILLIS_PER_SECOND = 1000L;

    private final AdminClientFactory adminClientFactory;

    public KafkaDlqTopicCreator() {
        this(AdminClient::create);
    }

    KafkaDlqTopicCreator(AdminClientFactory adminClientFactory) {
        this.adminClientFactory = adminClientFactory;
    }

    /**
     * Creates the configured DLQ topic when {@code DLQ_KAFKA_TOPIC_CREATE} is true
     * and the topic is not already present.
     *
     * @param config         DLQ kafka producer config
     * @param configuration  raw firehose configuration map
     * @param instrumentation logger/metrics helper
     */
    public void ensureTopic(DlqKafkaProducerConfig config, Map<String, String> configuration, FirehoseInstrumentation instrumentation) {
        if (!config.isDlqKafkaTopicCreate()) {
            return;
        }
        String topic = config.getDlqKafkaTopic();
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("DLQ_KAFKA_TOPIC must be set when DLQ_KAFKA_TOPIC_CREATE=true");
        }
        int retentionSeconds = config.getDlqKafkaTopicRetention();
        if (retentionSeconds <= 0) {
            throw new IllegalArgumentException("DLQ_KAFKA_TOPIC_RETENTION must be a positive number of seconds");
        }
        Properties properties = KafkaUtils.getDlqKafkaAdminProperties(KafkaProducerTypesMetadata.DLQ, config, configuration);
        try (AdminClient adminClient = adminClientFactory.create(properties)) {
            ensureTopic(adminClient, topic, retentionSeconds, instrumentation);
        }
    }

    void ensureTopic(AdminClient adminClient, String topic, int retentionSeconds, FirehoseInstrumentation instrumentation) {
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topic)) {
                instrumentation.logInfo("DLQ kafka topic {} already exists, skipping create", topic);
                return;
            }
            long retentionMs = retentionSeconds * MILLIS_PER_SECOND;
            NewTopic newTopic = new NewTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR)
                    .configs(Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs)));
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            instrumentation.logInfo("Created DLQ kafka topic {} with partitions={}, replicationFactor={}, retentionMs={}",
                    topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR, retentionMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating DLQ kafka topic " + topic, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                instrumentation.logInfo("DLQ kafka topic {} already exists, skipping create", topic);
                return;
            }
            throw new IllegalStateException("Failed to auto-create DLQ kafka topic " + topic, e);
        }
    }

    interface AdminClientFactory {
        AdminClient create(Properties properties);
    }
}
