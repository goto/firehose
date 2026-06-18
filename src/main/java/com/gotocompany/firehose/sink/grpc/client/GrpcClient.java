package com.gotocompany.firehose.sink.grpc.client;


import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.google.protobuf.DynamicMessage;

import com.gotocompany.firehose.metrics.Metrics;

import com.gotocompany.firehose.proto.ProtoToMetadataMapper;
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.Status;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;


/**
 * Custom GRPC client for all GRPC communication.
 */
public class GrpcClient {

    /** Instrumentation used to log calls and emit error metrics. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Bound gRPC sink configuration. */
    private final GrpcSinkConfig grpcSinkConfig;
    /** Stencil client used to parse the gRPC response payload. */
    private StencilClient stencilClient;
    /** Managed channel over which the unary calls are made. */
    private ManagedChannel managedChannel;
    /** Descriptor of the unary method, using raw byte-array marshalling. */
    private final MethodDescriptor<byte[], byte[]> methodDescriptor;
    /** Empty response returned when a call fails, signalling failure to the sink. */
    private final DynamicMessage emptyResponse;
    /** Maps configured proto fields into additional gRPC metadata. */
    private final ProtoToMetadataMapper protoToMetadataMapper;

    /**
     * Creates a gRPC client and builds the unary method descriptor from configuration.
     *
     * @param firehoseInstrumentation instrumentation used to log calls and emit metrics
     * @param grpcSinkConfig          the bound gRPC sink configuration
     * @param managedChannel          the channel over which calls are made
     * @param stencilClient           Stencil client used to parse responses
     * @param protoToMetadataMapper   mapper that derives extra gRPC metadata from the payload
     */
    public GrpcClient(FirehoseInstrumentation firehoseInstrumentation,
                      GrpcSinkConfig grpcSinkConfig,
                      ManagedChannel managedChannel,
                      StencilClient stencilClient,
                      ProtoToMetadataMapper protoToMetadataMapper) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.grpcSinkConfig = grpcSinkConfig;
        this.stencilClient = stencilClient;
        this.managedChannel = managedChannel;
        this.protoToMetadataMapper = protoToMetadataMapper;
        MethodDescriptor.Marshaller<byte[]> marshaller = getMarshaller();
        this.methodDescriptor = MethodDescriptor.newBuilder(marshaller, marshaller)
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(grpcSinkConfig.getSinkGrpcMethodUrl())
                .build();
        this.emptyResponse = DynamicMessage.newBuilder(this.stencilClient.get(this.grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass())).build();
    }

    /**
     * Performs the unary gRPC call for a single message.
     *
     * <p>Builds the request metadata from the Kafka headers and configured proto fields, applies any
     * configured deadline and parses the response payload. On a transport error the failure is logged and
     * counted and an empty response is returned.
     *
     * @param logMessage the raw protobuf payload sent as the request
     * @param headers    the Kafka record headers forwarded as gRPC metadata
     * @return the parsed response, or an empty response when the call fails
     */
    public DynamicMessage execute(byte[] logMessage, Headers headers) {
        Metadata metadata = buildMetadata(headers, logMessage);
        try {
            Channel decoratedChannel = ClientInterceptors.intercept(managedChannel,
                    MetadataUtils.newAttachHeadersInterceptor(metadata));
            firehoseInstrumentation.logDebug("Calling gRPC with metadata: {}", metadata.toString());
            byte[] response = ClientCalls.blockingUnaryCall(
                    decoratedChannel,
                    methodDescriptor,
                    decoratedDefaultCallOptions(),
                    logMessage);
            return stencilClient.parse(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass(), response);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                firehoseInstrumentation.logError("gRPC configurations are incorrect: {}", sre.getMessage());
            } else {
                firehoseInstrumentation.logError("gRPC call failed with error message: {}", sre.getMessage());
            }
            firehoseInstrumentation.incrementCounter(Metrics.SINK_GRPC_ERROR_TOTAL,  "status=" + sre.getStatus().getCode());
        } catch (Exception e) {
            firehoseInstrumentation.logError("gRPC call failed with error message: {}", e.getMessage());
            firehoseInstrumentation.incrementCounter(Metrics.SINK_GRPC_ERROR_TOTAL, "status=UNIDENTIFIED");
        }
        return emptyResponse;
    }

    /**
     * Builds the gRPC metadata for a call from Kafka headers and configured proto fields.
     *
     * @param headers    the Kafka record headers to forward
     * @param logMessage the payload from which configured metadata fields are extracted
     * @return the combined gRPC metadata
     */
    protected Metadata buildMetadata(Headers headers, byte[] logMessage) {
        Metadata metadata = new Metadata();
        for (Header header : headers) {
            metadata.put(Metadata.Key.of(header.key(), Metadata.ASCII_STRING_MARSHALLER), new String(header.value()));
        }
        Metadata externalizedMetadata = protoToMetadataMapper.buildGrpcMetadata(logMessage);
        metadata.merge(externalizedMetadata);
        return metadata;
    }

    /**
     * Returns the call options, applying a deadline when one is configured.
     *
     * @return the default call options, optionally with a deadline
     */
    protected CallOptions decoratedDefaultCallOptions() {
        CallOptions defaultCallOption = CallOptions.DEFAULT;
        if (grpcSinkConfig.getSinkGrpcArgDeadlineMS() != null && grpcSinkConfig.getSinkGrpcArgDeadlineMS() > 0) {
            return defaultCallOption.withDeadlineAfter(grpcSinkConfig.getSinkGrpcArgDeadlineMS(), TimeUnit.MILLISECONDS);
        }
        return defaultCallOption;
    }

    /**
     * Builds a marshaller that passes request and response bodies through as raw bytes.
     *
     * @return a byte-array marshaller
     */
    private MethodDescriptor.Marshaller<byte[]> getMarshaller() {
        return new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                try {
                    return IOUtils.toByteArray(stream);
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }
}
