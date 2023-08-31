package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;


public interface GrpcSinkConfig extends AppConfig {

    @Config.Key("SINK_GRPC_SERVICE_HOST")
    String getSinkGrpcServiceHost();

    @Config.Key("SINK_GRPC_SERVICE_PORT")
    Integer getSinkGrpcServicePort();

    @Config.Key("SINK_GRPC_METHOD_URL")
    String getSinkGrpcMethodUrl();

    @Config.Key("SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS")
    String getSinkGrpcResponseSchemaProtoClass();

    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIME_MS")
    Integer getSinkGrpcArgKeepaliveTimeMS();

    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS")
    Integer getSinkGrpcArgKeepaliveTimeoutMS();

    @Config.Key("SINK_GRPC_ARG_DEADLINE_MS")
    Integer getSinkGrpcArgDeadlineMS();
}
