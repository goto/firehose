package com.gotocompany.firehose.sink.dlq.kafka;

import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDlqTopicCreatorTest {

    @Mock
    private AdminClient adminClient;

    @Mock
    private ListTopicsResult listTopicsResult;

    @Mock
    private CreateTopicsResult createTopicsResult;

    @Mock
    private KafkaFuture<java.util.Set<String>> namesFuture;

    @Mock
    private KafkaFuture<Void> createFuture;

    @Mock
    private FirehoseInstrumentation instrumentation;

    @Captor
    private ArgumentCaptor<Collection<NewTopic>> newTopicsCaptor;

    private KafkaDlqTopicCreator creator;

    @Before
    public void setUp() {
        creator = new KafkaDlqTopicCreator(properties -> adminClient);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(namesFuture);
        when(adminClient.createTopics(org.mockito.ArgumentMatchers.any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(createFuture);
    }

    @Test
    public void shouldSkipWhenTopicCreateDisabled() throws Exception {
        Map<String, String> configMap = baseConfig();
        configMap.put("DLQ_KAFKA_TOPIC_CREATE", "false");
        DlqKafkaProducerConfig config = ConfigFactory.create(DlqKafkaProducerConfig.class, configMap);

        KafkaDlqTopicCreator.AdminClientFactory factory = properties -> {
            throw new AssertionError("admin client should not be created");
        };
        new KafkaDlqTopicCreator(factory).ensureTopic(config, configMap, instrumentation);
    }

    @Test
    public void shouldNotCreateWhenTopicAlreadyExists() throws Exception {
        when(namesFuture.get()).thenReturn(new HashSet<>(Collections.singleton("orders-firehose-dlq")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", 604800, instrumentation);

        verify(adminClient, never()).createTopics(org.mockito.ArgumentMatchers.any());
    }

    @Test
    public void shouldCreateMissingTopicWithDagstreamDefaults() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenReturn(null);

        creator.ensureTopic(adminClient, "orders-firehose-dlq", 604800, instrumentation);

        verify(adminClient).createTopics(newTopicsCaptor.capture());
        NewTopic newTopic = newTopicsCaptor.getValue().iterator().next();
        assertEquals("orders-firehose-dlq", newTopic.name());
        assertEquals(KafkaDlqTopicCreator.DEFAULT_PARTITIONS, newTopic.numPartitions());
        assertEquals(KafkaDlqTopicCreator.DEFAULT_REPLICATION_FACTOR, newTopic.replicationFactor());
        assertEquals("604800000", newTopic.configs().get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    public void shouldIgnoreTopicExistsRace() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenThrow(new ExecutionException(new TopicExistsException("exists")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", 604800, instrumentation);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailWhenCreateFails() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenThrow(new ExecutionException(new RuntimeException("broker down")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", 604800, instrumentation);
    }

    @Test
    public void shouldCreateViaFactoryWhenEnabled() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenReturn(null);

        Map<String, String> configMap = baseConfig();
        configMap.put("DLQ_KAFKA_TOPIC_CREATE", "true");
        DlqKafkaProducerConfig config = ConfigFactory.create(DlqKafkaProducerConfig.class, configMap);

        creator.ensureTopic(config, configMap, instrumentation);

        verify(adminClient).createTopics(newTopicsCaptor.capture());
        assertEquals("orders-firehose-dlq", newTopicsCaptor.getValue().iterator().next().name());
        verify(adminClient).close();
    }

    @Test
    public void shouldDefaultCreateDisabledAndRetentionSevenDays() {
        DlqKafkaProducerConfig config = ConfigFactory.create(DlqKafkaProducerConfig.class, baseConfig());
        assertFalse(config.isDlqKafkaTopicCreate());
        assertEquals(Integer.valueOf(604800), config.getDlqKafkaTopicRetention());
    }

    private static Map<String, String> baseConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_KAFKA_BROKERS", "localhost:9092");
        configMap.put("DLQ_KAFKA_TOPIC", "orders-firehose-dlq");
        return configMap;
    }
}
