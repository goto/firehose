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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
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

    private static final String PERSIST_CERT_DIRECTORY_PATH = "/usr/local/share/ca-certificates";

    private static File createCertFileFromBase64(String base64Cert) throws IOException {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Cert);
        File certDirectory = new File(PERSIST_CERT_DIRECTORY_PATH);
        if (!certDirectory.exists()) {
            certDirectory.mkdirs();
        }
        File certFile = Files.createTempFile(certDirectory.toPath(), "client-cert", ".crt").toFile();
        log.info("Created certificate file at {}", certFile.getAbsolutePath());
        try (FileOutputStream fos = new FileOutputStream(certFile)) {
            fos.write(decodedBytes);
        }
        return certFile;
    }

    private static SslContext buildClientSslContext(String base64Cert) {
        try {
            File certFile = createCertFileFromBase64(base64Cert);
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().trustManager(certFile);
            return GrpcSslContexts
                    .configure(sslContextBuilder)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to build SSL context due to an IO error while creating the certificate file.", e);
        }
    }

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

    private static PayloadEvaluator<Message> instantiatePayloadEvaluator(GrpcSinkConfig grpcSinkConfig, StencilClient stencilClient) {
        return new GrpcResponseCelPayloadEvaluator(
                stencilClient.get(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass()),
                grpcSinkConfig.getSinkGrpcResponseRetryCELExpression());

    }
}
