package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.enums.InputSchemaType;
import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.config.converter.InputSchemaTypeConverter;
import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import com.gotocompany.firehose.config.converter.SchemaRegistryHeadersConverter;
import com.gotocompany.firehose.config.converter.SchemaRegistryRefreshConverter;
import com.gotocompany.firehose.config.converter.SinkTypeConverter;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;

import org.aeonbits.owner.Config;
import org.apache.http.Header;

import java.util.List;
import java.util.Properties;

/**
 * Base owner configuration shared by every Firehose deployment, independent of the selected sink.
 *
 * <p>It extends the owner {@link org.aeonbits.owner.Config} contract and declares the cross-cutting
 * settings used throughout Firehose: the active {@link com.gotocompany.firehose.config.enums.SinkType},
 * application threading, Stencil schema-registry access, input-schema parsing and the retry/backoff
 * policy. Sink-specific configuration interfaces extend this one so these settings are available
 * everywhere. Each accessor maps to an environment variable through {@code @Key} and falls back to
 * its {@code @DefaultValue} when the variable is unset.
 */
public interface AppConfig extends Config {

    /**
     * Returns the destination sink Firehose streams messages to, set by {@code SINK_TYPE} and
     * converted by {@link com.gotocompany.firehose.config.converter.SinkTypeConverter}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.SinkType}
     */
    @Key("SINK_TYPE")
    @ConverterClass(SinkTypeConverter.class)
    SinkType getSinkType();

    /**
     * Returns the number of consumer worker threads Firehose runs in parallel, set by
     * {@code APPLICATION_THREAD_COUNT} and defaulting to {@code 1}.
     *
     * @return the application thread count
     */
    @Key("APPLICATION_THREAD_COUNT")
    @DefaultValue("1")
    Integer getApplicationThreadCount();

    /**
     * Returns the delay in milliseconds granted to worker threads to clean up during shutdown,
     * set by {@code APPLICATION_THREAD_CLEANUP_DELAY} and defaulting to {@code 2000}.
     *
     * @return the thread cleanup delay in milliseconds
     */
    @Key("APPLICATION_THREAD_CLEANUP_DELAY")
    @DefaultValue("2000")
    Integer getApplicationThreadCleanupDelay();

    /**
     * Indicates whether protobuf descriptors are fetched from the Stencil schema registry rather
     * than from bundled classes, set by {@code SCHEMA_REGISTRY_STENCIL_ENABLE} and defaulting to
     * {@code false}.
     *
     * @return {@code true} if the Stencil schema registry is enabled
     */
    @Key("SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    /**
     * Returns the timeout in milliseconds for a single Stencil schema-fetch request, set by
     * {@code SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS} and defaulting to {@code 10000}.
     *
     * @return the Stencil fetch timeout in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSchemaRegistryStencilFetchTimeoutMs();

    /**
     * Returns the number of times a failed Stencil schema fetch is retried, set by
     * {@code SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES} and defaulting to {@code 4}.
     *
     * @return the Stencil fetch retry count
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSchemaRegistryStencilFetchRetries();

    /**
     * Returns the minimum backoff in milliseconds between Stencil fetch retries, set by
     * {@code SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS} and defaulting to {@code 60000}.
     *
     * @return the minimum Stencil fetch backoff in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSchemaRegistryStencilFetchBackoffMinMs();

    /**
     * Returns the strategy controlling how the Stencil descriptor cache is refreshed, set by
     * {@code SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY}, converted by
     * {@link com.gotocompany.firehose.config.converter.SchemaRegistryRefreshConverter} and defaulting
     * to {@code VERSION_BASED_REFRESH}.
     *
     * @return the configured {@link com.gotocompany.stencil.cache.SchemaRefreshStrategy}
     */
    @Key("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    /**
     * Returns the HTTP headers attached to Stencil schema-registry requests, set by
     * {@code SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS} as a comma-separated list of {@code name:value}
     * pairs that is tokenized and converted by
     * {@link com.gotocompany.firehose.config.converter.SchemaRegistryHeadersConverter}; it defaults
     * to an empty list.
     *
     * @return the list of schema-registry request headers
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryFetchHeaders();

    /**
     * Indicates whether the Stencil cache is refreshed automatically in the background, set by
     * {@code SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH} and defaulting to {@code false}.
     *
     * @return {@code true} if Stencil cache auto-refresh is enabled
     */
    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("false")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    /**
     * Returns the time-to-live in milliseconds for entries in the Stencil cache, set by
     * {@code SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS} and defaulting to {@code 900000}.
     *
     * @return the Stencil cache TTL in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    /**
     * Returns the Stencil schema-registry URL(s) Firehose fetches descriptors from, set by
     * {@code SCHEMA_REGISTRY_STENCIL_URLS}.
     *
     * @return the Stencil registry URLs
     */
    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    String getSchemaRegistryStencilUrls();

    /**
     * Returns the fully-qualified protobuf class used to deserialize input messages, set by
     * {@code INPUT_SCHEMA_PROTO_CLASS}.
     *
     * @return the input proto message class name
     */
    @Key("INPUT_SCHEMA_PROTO_CLASS")
    String getInputSchemaProtoClass();

    /**
     * Returns the encoding of input messages, set by {@code INPUT_SCHEMA_DATA_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.InputSchemaTypeConverter} and defaulting to
     * {@code PROTOBUF}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.InputSchemaType}
     */
    @Key("INPUT_SCHEMA_DATA_TYPE")
    @DefaultValue("PROTOBUF")
    @ConverterClass(InputSchemaTypeConverter.class)
    InputSchemaType getInputSchemaType();

    /**
     * Returns the mapping from protobuf field indices to output column names, set by
     * {@code INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING} and parsed from JSON by
     * {@link com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter}.
     *
     * @return the proto-index-to-column mapping as a {@code Properties} tree
     */
    @Key("INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getInputSchemaProtoToColumnMapping();

    /**
     * Returns which part of a Kafka record is parsed against the schema, set by
     * {@code KAFKA_RECORD_PARSER_MODE} and defaulting to {@code message} (the alternative being
     * {@code key}).
     *
     * @return the Kafka record parser mode
     */
    @Key("KAFKA_RECORD_PARSER_MODE")
    @DefaultValue("message")
    String getKafkaRecordParserMode();

    /**
     * Indicates whether Jaeger distributed tracing is enabled, set by {@code TRACE_JAEGAR_ENABLE} and
     * defaulting to {@code false}.
     *
     * @return {@code true} if Jaeger tracing is enabled
     */
    @Key("TRACE_JAEGAR_ENABLE")
    @DefaultValue("false")
    Boolean isTraceJaegarEnable();

    /**
     * Returns the initial delay in milliseconds for the exponential retry backoff, set by
     * {@code RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS} and defaulting to {@code 10}.
     *
     * @return the initial retry backoff in milliseconds
     */
    @Key("RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS")
    @DefaultValue("10")
    Integer getRetryExponentialBackoffInitialMs();

    /**
     * Returns the multiplier applied to the retry backoff after each attempt, set by
     * {@code RETRY_EXPONENTIAL_BACKOFF_RATE} and defaulting to {@code 2}.
     *
     * @return the exponential backoff rate
     */
    @Key("RETRY_EXPONENTIAL_BACKOFF_RATE")
    @DefaultValue("2")
    Integer getRetryExponentialBackoffRate();

    /**
     * Returns the maximum delay in milliseconds the exponential retry backoff can grow to, set by
     * {@code RETRY_EXPONENTIAL_BACKOFF_MAX_MS} and defaulting to {@code 60000}.
     *
     * @return the maximum retry backoff in milliseconds
     */
    @Key("RETRY_EXPONENTIAL_BACKOFF_MAX_MS")
    @DefaultValue("60000")
    Integer getRetryExponentialBackoffMaxMs();

    /**
     * Indicates whether a message is failed once its retry attempts are exhausted, set by
     * {@code RETRY_FAIL_AFTER_MAX_ATTEMPTS_ENABLE} and defaulting to {@code false}.
     *
     * @return {@code true} if exhausting retries should fail the message
     */
    @Key("RETRY_FAIL_AFTER_MAX_ATTEMPTS_ENABLE")
    @DefaultValue("false")
    boolean getRetryFailAfterMaxAttemptsEnable();

    /**
     * Returns the maximum number of retry attempts for a failed message, set by
     * {@code RETRY_MAX_ATTEMPTS} and defaulting to {@code 2147483647} ({@code Integer.MAX_VALUE}).
     *
     * @return the maximum retry attempts
     */
    @Key("RETRY_MAX_ATTEMPTS")
    @DefaultValue("2147483647")
    Integer getRetryMaxAttempts();

    /**
     * Indicates whether unknown protobuf fields are tolerated while parsing input messages, set by
     * {@code INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE} and defaulting to {@code true}.
     *
     * @return {@code true} if unknown proto fields are allowed
     */
    @Key("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")
    @DefaultValue("true")
    boolean getInputSchemaProtoAllowUnknownFieldsEnable();
}
