package com.gotocompany.firehose.sink.httpv2;

import com.gotocompany.firehose.config.enums.KafkaConsumerMode;

import java.util.Map;

/**
 * Utility helpers for configuring the HTTP v2 sink.
 *
 * <p>Used while bootstrapping the HTTP v2 sink to derive connection-related settings from the Kafka
 * consumer mode before the sink configuration is bound.
 */
public class HttpV2SinkUtils {

    /**
     * Adds HTTP v2 connection settings to the environment map based on the Kafka consumer mode.
     *
     * <p>In {@code SYNC} mode the maximum number of connections is fixed at one; in {@code ASYNC} mode it is
     * taken from the configured sink thread-pool size (defaulting to one). The consumer mode is read from
     * {@code SOURCE_KAFKA_CONSUMER_MODE} and defaults to {@code SYNC}.
     *
     * @param env the mutable environment configuration map to augment in place
     * @throws IllegalArgumentException if the consumer mode is neither {@code SYNC} nor {@code ASYNC}
     */
    public static void addAdditionalConfigsForHttpV2Sink(Map<String, String> env) {

        System.out.println(env.getOrDefault("SOURCE_KAFKA_CONSUMER_MODE", "SYNC").toUpperCase());
        switch (KafkaConsumerMode.valueOf(env.getOrDefault("SOURCE_KAFKA_CONSUMER_MODE", "SYNC").toUpperCase())) {
            case SYNC:
                env.put("SINK_HTTPV2_MAX_CONNECTIONS", "1");
                break;

            case ASYNC:
                env.put("SINK_HTTPV2_MAX_CONNECTIONS", env.getOrDefault("SINK_POOL_NUM_THREADS", "1"));
                break;
            default:
                throw new IllegalArgumentException("Consumer mode should be async or sync");

        }
    }
}
