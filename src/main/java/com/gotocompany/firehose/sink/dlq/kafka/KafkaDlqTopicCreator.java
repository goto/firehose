package com.gotocompany.firehose.sink.dlq.kafka;

import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.utils.KafkaProducerTypesMetadata;
import com.gotocompany.firehose.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Creates a Kafka DLQ topic when it is missing. Partition count, replication
 * factor, and retention come from the broker's cluster defaults.
 */
public class KafkaDlqTopicCreator {

    private final AdminClientFactory adminClientFactory;

    public KafkaDlqTopicCreator() {
        this(AdminClient::create);
    }

    KafkaDlqTopicCreator(AdminClientFactory adminClientFactory) {
        this.adminClientFactory = adminClientFactory;
    }

    /**
     * Creates the configured DLQ topic when it is not already present.
     *
     * @param config         DLQ kafka producer config
     * @param configuration  raw firehose configuration map
     * @param instrumentation logger/metrics helper
     */
    public void ensureTopic(DlqKafkaProducerConfig config, Map<String, String> configuration, FirehoseInstrumentation instrumentation) {
        String topic = config.getDlqKafkaTopic();
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("DLQ_KAFKA_TOPIC must be set when Kafka DLQ is enabled");
        }
        Properties properties = KafkaUtils.getDlqKafkaAdminProperties(KafkaProducerTypesMetadata.DLQ, config, configuration);
        try (AdminClient adminClient = adminClientFactory.create(properties)) {
            ensureTopic(adminClient, topic, instrumentation);
        }
    }

    void ensureTopic(AdminClient adminClient, String topic, FirehoseInstrumentation instrumentation) {
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topic)) {
                instrumentation.logInfo("DLQ kafka topic {} already exists, skipping create", topic);
                return;
            }
            NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            instrumentation.logInfo("Created DLQ kafka topic {} using broker defaults", topic);
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
