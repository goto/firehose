package com.gotocompany.firehose.config;

import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.converter.GrpcMetadataConverter;
import com.gotocompany.firehose.config.converter.GrpcSinkRetryErrorTypeConverter;
import org.aeonbits.owner.Config;

import java.util.Map;


/**
 * Owner configuration for the gRPC sink, which forwards consumed messages to a gRPC service.
 *
 * <p>It supplies the target host, port and method, the response schema, the keepalive and deadline
 * tunables, the retry policy (a CEL expression plus a retryable error type), call metadata and TLS
 * settings. Each accessor maps to an environment variable via {@code @Key} and, where present, falls
 * back to its {@code @DefaultValue}.
 */
public interface GrpcSinkConfig extends AppConfig {

    /**
     * Returns the host of the target gRPC service, set by {@code SINK_GRPC_SERVICE_HOST}.
     *
     * @return the gRPC service host
     */
    @Config.Key("SINK_GRPC_SERVICE_HOST")
    String getSinkGrpcServiceHost();

    /**
     * Returns the port of the target gRPC service, set by {@code SINK_GRPC_SERVICE_PORT}.
     *
     * @return the gRPC service port
     */
    @Config.Key("SINK_GRPC_SERVICE_PORT")
    Integer getSinkGrpcServicePort();

    /**
     * Returns the fully-qualified gRPC method (service and method) to invoke, set by
     * {@code SINK_GRPC_METHOD_URL}.
     *
     * @return the gRPC method URL
     */
    @Config.Key("SINK_GRPC_METHOD_URL")
    String getSinkGrpcMethodUrl();

    /**
     * Returns the fully-qualified protobuf class used to deserialize the gRPC response, set by
     * {@code SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS}.
     *
     * @return the gRPC response proto class name
     */
    @Config.Key("SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS")
    String getSinkGrpcResponseSchemaProtoClass();

    /**
     * Returns the gRPC channel keepalive ping interval in milliseconds, set by
     * {@code SINK_GRPC_ARG_KEEPALIVE_TIME_MS} and defaulting to {@code 9223372036854775807}
     * ({@code Long.MAX_VALUE}, effectively disabling keepalive pings).
     *
     * @return the keepalive time in milliseconds
     */
    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIME_MS")
    @Config.DefaultValue("9223372036854775807")
    Long getSinkGrpcArgKeepaliveTimeMS();

    /**
     * Returns how long in milliseconds a keepalive ping waits for an acknowledgement before the
     * connection is considered dead, set by {@code SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS} and
     * defaulting to {@code 20000}.
     *
     * @return the keepalive timeout in milliseconds
     */
    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS")
    @DefaultValue("20000")
    Long getSinkGrpcArgKeepaliveTimeoutMS();

    /**
     * Returns the per-call deadline in milliseconds after which a gRPC request is cancelled, set by
     * {@code SINK_GRPC_ARG_DEADLINE_MS}.
     *
     * @return the gRPC call deadline in milliseconds
     */
    @Config.Key("SINK_GRPC_ARG_DEADLINE_MS")
    Long getSinkGrpcArgDeadlineMS();

    /**
     * Returns the CEL (Common Expression Language) expression evaluated against the gRPC response to
     * decide whether a call should be retried, set by {@code SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION}
     * and defaulting to {@code true}.
     *
     * @return the retry CEL expression
     */
    @Config.Key("SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION")
    @DefaultValue("true")
    String getSinkGrpcResponseRetryCELExpression();

    /**
     * Returns the depot error type assigned to a failed gRPC call so the retry layer can decide how
     * to treat it, set by {@code SINK_GRPC_RESPONSE_RETRY_ERROR_TYPE}, converted by
     * {@link com.gotocompany.firehose.config.converter.GrpcSinkRetryErrorTypeConverter} and
     * defaulting to {@code DEFAULT_ERROR}.
     *
     * @return the configured retry {@link com.gotocompany.depot.error.ErrorType}
     */
    @Config.Key("SINK_GRPC_RESPONSE_RETRY_ERROR_TYPE")
    @DefaultValue("DEFAULT_ERROR")
    @ConverterClass(GrpcSinkRetryErrorTypeConverter.class)
    ErrorType getSinkGrpcRetryErrorType();

    /**
     * Returns the static metadata (headers) attached to every gRPC call, set by
     * {@code SINK_GRPC_METADATA} as a comma-separated list of {@code key:value} pairs that is parsed
     * by {@link com.gotocompany.firehose.config.converter.GrpcMetadataConverter}; it defaults to an
     * empty map.
     *
     * @return a map of gRPC metadata keys to values
     */
    @Key("SINK_GRPC_METADATA")
    @DefaultValue("")
    @ConverterClass(GrpcMetadataConverter.class)
    Map<String, String> getSinkGrpcMetadata();

    /**
     * Indicates whether the gRPC channel uses TLS, set by {@code SINK_GRPC_TLS_ENABLE} and
     * defaulting to {@code false}.
     *
     * @return {@code true} if TLS is enabled for the gRPC channel
     */
    @Config.Key("SINK_GRPC_TLS_ENABLE")
    @DefaultValue("false")
    boolean getSinkGrpcTlsEnable();

    /**
     * Returns the root CA certificate (PEM) used to verify the server when TLS is enabled, set by
     * {@code SINK_GRPC_ROOT_CA}.
     *
     * @return the gRPC root CA certificate
     */
    @Config.Key("SINK_GRPC_ROOT_CA")
    String getSinkGrpcRootCA();
}
