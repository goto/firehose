package com.gotocompany.firehose.sink;

import com.gotocompany.depot.bigquery.BigQuerySink;
import com.gotocompany.depot.bigquery.BigQuerySinkFactory;
import com.gotocompany.depot.bigtable.BigTableSink;
import com.gotocompany.depot.bigtable.BigTableSinkFactory;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.http.HttpSink;
import com.gotocompany.depot.log.LogSink;
import com.gotocompany.depot.log.LogSinkFactory;
import com.gotocompany.depot.maxcompute.MaxComputeSink;
import com.gotocompany.depot.maxcompute.MaxComputeSinkFactory;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.RedisSink;
import com.gotocompany.depot.redis.RedisSinkFactory;
import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.bigquery.BigquerySinkUtils;
import com.gotocompany.firehose.sink.blob.BlobSinkFactory;
import com.gotocompany.firehose.sink.elasticsearch.EsSinkFactory;
import com.gotocompany.firehose.sink.grpc.GrpcSinkFactory;
import com.gotocompany.firehose.sink.http.HttpSinkFactory;
import com.gotocompany.firehose.sink.httpv2.HttpV2SinkUtils;
import com.gotocompany.firehose.sink.influxdb.InfluxSinkFactory;
import com.gotocompany.firehose.sink.jdbc.JdbcSinkFactory;
import com.gotocompany.firehose.sink.mongodb.MongoSinkFactory;
import com.gotocompany.firehose.sink.prometheus.PromSinkFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory that builds the {@link Sink} implementation selected by the Firehose configuration.
 * <p>
 * Reads the configured {@link com.gotocompany.firehose.config.enums.SinkType} from
 * {@link com.gotocompany.firehose.config.KafkaConsumerConfig} and, in {@link #init()}, eagerly
 * initialises the underlying Depot sink factories that require it (for example log, Redis,
 * BigQuery, BigTable, HTTP v2 and MaxCompute). {@link #getSink()} then returns a ready-to-use
 * sink for that type, wrapping Depot-based sinks in a {@link GenericSink} and delegating to the
 * native Firehose factories (HTTP, gRPC, JDBC, InfluxDB, Elasticsearch, Prometheus, blob and
 * MongoDB) for the rest.
 * <p>
 * Configuration is sourced from the process environment and augmented by
 * {@link SinkFactoryUtils#addAdditionalConfigsForSinkConnectors(java.util.Map)}. Metrics are
 * reported through the supplied {@link com.gotocompany.depot.metrics.StatsDReporter}.
 *
 * @see Sink
 * @see GenericSink
 */
public class SinkFactory {
    /** Consumer configuration that determines which sink type to build. */
    private final KafkaConsumerConfig kafkaConsumerConfig;
    /** Reporter used to publish metrics for the created sinks. */
    private final StatsDReporter statsDReporter;
    /** Instrumentation for this factory's own logging. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Stencil client used for protobuf schema resolution. */
    private final StencilClient stencilClient;
    /** Offset manager handed to sinks that commit Kafka offsets themselves, such as the blob sink. */
    private final OffsetManager offsetManager;
    /** Connector configuration derived from the environment, shared by the Depot sink factories. */
    private final Map<String, String> config;
    /** Depot BigQuery sink factory; created by {@link #init()} when the sink type is BIGQUERY. */
    private BigQuerySinkFactory bigQuerySinkFactory;
    /** Depot BigTable sink factory; created by {@link #init()} when the sink type is BIGTABLE. */
    private BigTableSinkFactory bigTableSinkFactory;
    /** Depot log sink factory; created by {@link #init()} when the sink type is LOG. */
    private LogSinkFactory logSinkFactory;
    /** Depot Redis sink factory; created by {@link #init()} when the sink type is REDIS. */
    private RedisSinkFactory redisSinkFactory;
    /** Depot HTTP sink factory backing the HTTPV2 sink type; created by {@link #init()}. */
    private com.gotocompany.depot.http.HttpSinkFactory httpv2SinkFactory;
    /** Depot MaxCompute sink factory; created by {@link #init()} when the sink type is MAXCOMPUTE. */
    private MaxComputeSinkFactory maxComputeSinkFactory;

    /**
     * Creates a sink factory bound to the given configuration and collaborators.
     * <p>
     * Initialises this factory's own instrumentation and loads the connector configuration from
     * the environment via
     * {@link SinkFactoryUtils#addAdditionalConfigsForSinkConnectors(java.util.Map)}. Call
     * {@link #init()} before {@link #getSink()}.
     *
     * @param kafkaConsumerConfig the consumer configuration that selects the sink type
     * @param statsDReporter the reporter used to publish sink metrics
     * @param stencilClient the Stencil client used for protobuf schema resolution
     * @param offsetManager the offset manager passed to sinks that manage their own offsets
     */
    public SinkFactory(KafkaConsumerConfig kafkaConsumerConfig,
                       StatsDReporter statsDReporter,
                       StencilClient stencilClient,
                       OffsetManager offsetManager) {
        firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, SinkFactory.class);
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.offsetManager = offsetManager;
        this.config = SinkFactoryUtils.addAdditionalConfigsForSinkConnectors(System.getenv());
    }

    /**
     * Initialization method for all the sinks.
     */
    public void init() {
        switch (this.kafkaConsumerConfig.getSinkType()) {
            case JDBC:
            case HTTP:
            case INFLUXDB:
            case ELASTICSEARCH:
            case GRPC:
            case PROMETHEUS:
            case BLOB:
            case MONGODB:
                return;
            case LOG:
                logSinkFactory = new LogSinkFactory(config, statsDReporter);
                logSinkFactory.init();
                return;
            case REDIS:
                redisSinkFactory = new RedisSinkFactory(
                        ConfigFactory.create(RedisSinkConfig.class, config),
                        statsDReporter);
                redisSinkFactory.init();
                return;
            case BIGQUERY:
                BigquerySinkUtils.addMetadataColumns(config);
                bigQuerySinkFactory = new BigQuerySinkFactory(
                        ConfigFactory.create(BigQuerySinkConfig.class, config),
                        statsDReporter,
                        BigquerySinkUtils.getRowIDCreator());
                bigQuerySinkFactory.init();
                return;
            case BIGTABLE:
                bigTableSinkFactory = new BigTableSinkFactory(
                        ConfigFactory.create(BigTableSinkConfig.class, config),
                        statsDReporter);
                bigTableSinkFactory.init();
                return;
            case HTTPV2:
                HttpV2SinkUtils.addAdditionalConfigsForHttpV2Sink(config);
                httpv2SinkFactory = new com.gotocompany.depot.http.HttpSinkFactory(
                        ConfigFactory.create(HttpSinkConfig.class, config),
                        statsDReporter);
                httpv2SinkFactory.init();
                return;
            case MAXCOMPUTE:
                maxComputeSinkFactory = new MaxComputeSinkFactory(statsDReporter, stencilClient, config);
                maxComputeSinkFactory.init();
                return;
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }

    /**
     * Builds and returns the {@link Sink} for the configured sink type.
     * <p>
     * Native Firehose sinks are created by their respective factories, while Depot-backed sinks
     * are wrapped in a {@link GenericSink} that carries the appropriate instrumentation.
     * {@link #init()} must have been called first for the sink types that rely on a
     * pre-initialised Depot factory.
     *
     * @return a ready-to-use sink instance for the configured type
     * @throws ConfigurationException if the configured sink type is not supported
     */
    public Sink getSink() {
        SinkType sinkType = kafkaConsumerConfig.getSinkType();
        firehoseInstrumentation.logInfo("Sink Type: {}", sinkType);
        switch (sinkType) {
            case JDBC:
                return JdbcSinkFactory.create(config, statsDReporter, stencilClient);
            case HTTP:
                return HttpSinkFactory.create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return InfluxSinkFactory.create(config, statsDReporter, stencilClient);
            case LOG:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, LogSink.class), sinkType.name(), logSinkFactory.create());
            case ELASTICSEARCH:
                return EsSinkFactory.create(config, statsDReporter, stencilClient);
            case REDIS:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, RedisSink.class), sinkType.name(), redisSinkFactory.create());
            case GRPC:
                return GrpcSinkFactory.create(config, statsDReporter, stencilClient);
            case PROMETHEUS:
                return PromSinkFactory.create(config, statsDReporter, stencilClient);
            case BLOB:
                return BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
            case BIGQUERY:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, BigQuerySink.class), sinkType.name(), bigQuerySinkFactory.create());
            case BIGTABLE:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, BigTableSink.class), sinkType.name(), bigTableSinkFactory.create());
            case MONGODB:
                return MongoSinkFactory.create(config, statsDReporter, stencilClient);
            case HTTPV2:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, HttpSink.class), sinkType.name(), httpv2SinkFactory.create());
            case MAXCOMPUTE:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, MaxComputeSink.class), sinkType.name(), maxComputeSinkFactory.create());
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }
}
