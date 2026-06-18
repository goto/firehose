package com.gotocompany.firehose.sink.grpc;



import com.gotocompany.depot.metrics.StatsDReporter;

import com.google.protobuf.Message;
import com.gotocompany.firehose.config.AppConfig;

import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.evaluator.GrpcResponseCelPayloadEvaluator;
import com.gotocompany.firehose.evaluator.PayloadEvaluator;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import com.gotocompany.firehose.proto.ProtoToMetadataMapper;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigFactory;

import javax.net.ssl.SSLException;
import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */
@Slf4j
public class GrpcSinkFactory {

    /**
     * Builds a gRPC sink from the supplied configuration.
     *
     * <p>Binds the configuration to {@link GrpcSinkConfig}, builds a Netty-based managed channel (optionally
     * secured with TLS using a base64-encoded root CA), wires a {@link GrpcClient} with metadata mapping and
     * assembles the CEL-based retry evaluator.
     *
     * @param configuration  the raw key-value configuration the sink is bound from
     * @param statsDReporter reporter used to publish StatsD metrics for the sink and its collaborators
     * @param stencilClient  Stencil client used to resolve request, response and metadata schemas
     * @return a fully wired {@link GrpcSink} instance ready to receive messages
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        firehoseInstrumentation.logDebug(grpcSinkConfig);
        boolean isTlsEnabled = grpcConfig.getSinkGrpcTlsEnable();
        NettyChannelBuilder managedChannelBuilder = NettyChannelBuilder
                .forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
                .keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS);
        if (isTlsEnabled) {
            String base64Cert = grpcConfig.getSinkGrpcRootCA();
            SslContext sslContext = buildClientSslContext(base64Cert);
            firehoseInstrumentation.logInfo("SSL Context created successfully.");
            managedChannelBuilder.sslContext(sslContext);
        } else {
            managedChannelBuilder.usePlaintext();
        }
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configuration);
        ProtoToMetadataMapper protoToMetadataMapper = new ProtoToMetadataMapper(stencilClient.get(appConfig.getInputSchemaProtoClass()), grpcConfig.getSinkGrpcMetadata());
        GrpcClient grpcClient = new GrpcClient(
                new FirehoseInstrumentation(statsDReporter, GrpcClient.class),
                grpcConfig,
                managedChannelBuilder.build(),
                stencilClient, protoToMetadataMapper);
        firehoseInstrumentation.logInfo("gRPC Client created successfully.");
        PayloadEvaluator<Message> grpcResponseRetryEvaluator = instantiatePayloadEvaluator(grpcConfig, stencilClient);
        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient, grpcConfig, grpcResponseRetryEvaluator);
    }

    /**
     * Builds the client SSL context from a base64-encoded root CA certificate.
     *
     * @param base64Cert the base64-encoded PEM root CA certificate
     * @return the configured SSL context
     * @throws RuntimeException if the certificate cannot be decoded or the context cannot be built
     */
    private static SslContext buildClientSslContext(String base64Cert) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(base64Cert);
            ByteArrayInputStream certInputStream = new ByteArrayInputStream(decodedBytes);
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().trustManager(certInputStream);
            return GrpcSslContexts.configure(sslContextBuilder).build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds the CEL payload evaluator that decides whether a gRPC response is retryable.
     *
     * @param grpcSinkConfig the bound gRPC sink configuration supplying the response schema and CEL expression
     * @param stencilClient  Stencil client used to resolve the response schema
     * @return the retry evaluator
     */
    private static PayloadEvaluator<Message> instantiatePayloadEvaluator(GrpcSinkConfig grpcSinkConfig, StencilClient stencilClient) {
        return new GrpcResponseCelPayloadEvaluator(
                stencilClient.get(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass()),
                grpcSinkConfig.getSinkGrpcResponseRetryCELExpression());

    }
}
