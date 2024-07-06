package com.gotocompany.firehose.sink.grpc;


import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.stencil.client.StencilClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
//import io.grpc.netty.GrpcSslContexts;
//import io.grpc.netty.NettyChannelBuilder;
//import io.netty.handler.ssl.SslContext;
//import io.netty.handler.ssl.SslContextBuilder;
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
public class GrpcSinkFactory {

    private static final String CERT_DIRECTORY = "/usr/local/share/ca-certificates";

    private static void createTempFileFromBase64(String base64Cert) throws IOException {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Cert);
        File certDirectory = new File(CERT_DIRECTORY);
        if (!certDirectory.exists()) {
            certDirectory.mkdirs();
        }
        File tempFile = Files.createTempFile(certDirectory.toPath(), "goto-root-ca", ".crt").toFile();
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(decodedBytes);
        }
    }

//    private static SslContext buildClientSslContext(String base64Cert) {
//        try {
//            File certFile = createTempFileFromBase64(base64Cert);
//            return GrpcSslContexts
//                    .configure(SslContextBuilder.forClient().trustManager(certFile))
//                    .build();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        firehoseInstrumentation.logDebug(grpcSinkConfig);
        boolean isTlsEnable = grpcConfig.getSinkGrpcTlsEnable();

        ManagedChannel managedChannel;
        if (isTlsEnable) {
            try {
                System.out.println("hello, with TLS");
                String base64Cert = grpcConfig.getSinkGrpcRootCA();
                createTempFileFromBase64(base64Cert);
//            SslContext sslContext = buildClientSslContext(base64Cert);
//            System.out.println("SSL Context created successfully.");
//            managedChannel = NettyChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
//                    .keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS)
//                    .keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS)
//                    .sslContext(sslContext)
//                    .build();
                managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
                        .keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS)
                        .build();
            } catch (IOException e) {
            throw new RuntimeException(e);
        }
        } else {
            System.out.println("hello, no TLS");
            managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
                    .keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS)
                    .keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS)
                    .usePlaintext()
                    .build();
        }

        GrpcClient grpcClient = new GrpcClient(new FirehoseInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        firehoseInstrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

}



