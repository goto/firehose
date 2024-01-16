package com.gotocompany.firehose.sink.grpc.client;


import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.google.protobuf.DynamicMessage;

import com.gotocompany.firehose.metrics.Metrics;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
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

    private FirehoseInstrumentation firehoseInstrumentation;
    private final GrpcSinkConfig grpcSinkConfig;
    private StencilClient stencilClient;
    private ManagedChannel managedChannel;
    private MethodDescriptor<byte[], byte[]> methodeBuilder;

    public GrpcClient(FirehoseInstrumentation firehoseInstrumentation, GrpcSinkConfig grpcSinkConfig, ManagedChannel managedChannel, StencilClient stencilClient) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.grpcSinkConfig = grpcSinkConfig;
        this.stencilClient = stencilClient;
        this.managedChannel = managedChannel;
    }

    public void initialize() {
        MethodDescriptor.Marshaller<byte[]> marshaller = getMarshaller();
        this.methodeBuilder = MethodDescriptor.newBuilder(marshaller, marshaller)
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(grpcSinkConfig.getSinkGrpcMethodUrl())
                .build();
    }

    public DynamicMessage execute(byte[] logMessage, Headers headers) {

        CallOptions callOption = CallOptions.DEFAULT;
        try {
            Metadata metadata = new Metadata();
            for (Header header : headers) {
                metadata.put(Metadata.Key.of(header.key(), Metadata.ASCII_STRING_MARSHALLER), new String(header.value()));
            }
            Channel decoratedChannel = ClientInterceptors.intercept(managedChannel,
                     MetadataUtils.newAttachHeadersInterceptor(metadata));
            callOption = decorateCallOptions(callOption);
            byte[] response = ClientCalls.blockingUnaryCall(
                    decoratedChannel,
                    methodeBuilder,
                    callOption,
                    logMessage);

            return stencilClient.parse(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass(), response);

        } catch (StatusRuntimeException sre) {
            firehoseInstrumentation.logWarn(sre.getMessage());
            firehoseInstrumentation.incrementCounter(Metrics.SINK_GRPC_ERROR_TOTAL,  "status=" + sre.getStatus().getCode());
        } catch (Exception e) {
            e.printStackTrace();
            firehoseInstrumentation.logWarn(e.getMessage());
            firehoseInstrumentation.incrementCounter(Metrics.SINK_GRPC_ERROR_TOTAL, "status=UNIDENTIFIED");
        }
        return DynamicMessage.newBuilder(this.stencilClient.get(this.grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass())).build();
    }

    protected CallOptions decorateCallOptions(CallOptions defaultCallOptions) {
        if (grpcSinkConfig.getSinkGrpcArgDeadlineMS() != null && grpcSinkConfig.getSinkGrpcArgDeadlineMS() > 0) {
            defaultCallOptions = defaultCallOptions.withDeadlineAfter(grpcSinkConfig.getSinkGrpcArgDeadlineMS(), TimeUnit.MILLISECONDS);
        }
        return defaultCallOptions;
    }

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
