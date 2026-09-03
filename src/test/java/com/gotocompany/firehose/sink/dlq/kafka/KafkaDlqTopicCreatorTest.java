package com.gotocompany.firehose.sink.dlq.kafka;

import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
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
import static org.junit.Assert.assertTrue;
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
    public void shouldNotCreateWhenTopicAlreadyExists() throws Exception {
        when(namesFuture.get()).thenReturn(new HashSet<>(Collections.singleton("orders-firehose-dlq")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", instrumentation);

        verify(adminClient, never()).createTopics(org.mockito.ArgumentMatchers.any());
    }

    @Test
    public void shouldCreateMissingTopicWithBrokerDefaults() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenReturn(null);

        creator.ensureTopic(adminClient, "orders-firehose-dlq", instrumentation);

        verify(adminClient).createTopics(newTopicsCaptor.capture());
        NewTopic newTopic = newTopicsCaptor.getValue().iterator().next();
        assertEquals("orders-firehose-dlq", newTopic.name());
        assertEquals(-1, newTopic.numPartitions());
        assertEquals(-1, newTopic.replicationFactor());
        assertTrue(newTopic.configs() == null || newTopic.configs().isEmpty());
    }

    @Test
    public void shouldIgnoreTopicExistsRace() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenThrow(new ExecutionException(new TopicExistsException("exists")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", instrumentation);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailWhenCreateFails() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenThrow(new ExecutionException(new RuntimeException("broker down")));

        creator.ensureTopic(adminClient, "orders-firehose-dlq", instrumentation);
    }

    @Test
    public void shouldCreateViaFactoryWhenTopicMissing() throws Exception {
        when(namesFuture.get()).thenReturn(Collections.emptySet());
        when(createFuture.get()).thenReturn(null);

        Map<String, String> configMap = baseConfig();
        DlqKafkaProducerConfig config = ConfigFactory.create(DlqKafkaProducerConfig.class, configMap);

        creator.ensureTopic(config, configMap, instrumentation);

        verify(adminClient).createTopics(newTopicsCaptor.capture());
        assertEquals("orders-firehose-dlq", newTopicsCaptor.getValue().iterator().next().name());
        verify(adminClient).close();
    }

    private static Map<String, String> baseConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_KAFKA_BROKERS", "localhost:9092");
        configMap.put("DLQ_KAFKA_TOPIC", "orders-firehose-dlq");
        return configMap;
    }
}
