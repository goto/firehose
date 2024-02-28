package com.gotocompany.firehose.sink.httpv2;

import java.util.Map;

public class HttpV2SinkUtils {
    private static final String SYNC_MODE = "sync";

    public static void addAdditionalConfigsForHttpV2Sink(Map<String, String> env) {
        if (SYNC_MODE.equalsIgnoreCase(env.get("SOURCE_KAFKA_CONSUMER_MODE"))) {
            env.put("SINK_HTTPV2_MAX_CONNECTIONS", "1");
        } else {
            env.put("SINK_HTTPV2_MAX_CONNECTIONS", env.get("SINK_POOL_NUM_THREADS"));
        }
    }
}
