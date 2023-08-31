package com.gotocompany.firehose.sink.grpc;


import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.AbstractSink;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */
public class GrpcSinkFactory {

    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        firehoseInstrumentation.logDebug(grpcSinkConfig);

        ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
                .usePlaintext();
        ManagedChannel managedChannel = decorateManagedChannelBuilder(grpcConfig, managedChannelBuilder).build();

        GrpcClient grpcClient = new GrpcClient(new FirehoseInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        grpcClient.initialize();
        firehoseInstrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

    protected static ManagedChannelBuilder<?> decorateManagedChannelBuilder(GrpcSinkConfig grpcConfig, ManagedChannelBuilder<?> channelBuilder) {
        if (grpcConfig.getSinkGrpcArgKeepaliveTimeMS() != null && grpcConfig.getSinkGrpcArgKeepaliveTimeMS() > 0) {
            channelBuilder = channelBuilder.keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS);
        }
        if (grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS() != null && grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS() > 0) {
            channelBuilder = channelBuilder.keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS);
        }
        return channelBuilder;
    }

}
