package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import com.gotocompany.firehose.exception.OperationNotSupportedException;
import io.grpc.Metadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

public class ProtoToMetadataMapperTest {

    private ProtoToMetadataMapper protoToMetadataMapper;

    @Before
    public void setup() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("someField", "someValue");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + '-' + GenericResponse.errors[0].code + '-' + string(GenericResponse.code))");
        template.put("entity", "$GenericResponse.errors[0].entity");
        this.protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                template
        );
    }

    @Test
    public void shouldBuildDynamicMetadataWithCorrectPlaceholders() {
        GenericResponse payload = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("detail_of_error")
                .setCode(100)
                .addErrors(GenericError.newBuilder()
                        .setCode("404")
                        .setCause("not_found")
                        .build())
                .build();

        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(payload.toByteArray());

        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("false", metadata.get(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("not_found-404-100", metadata.get(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("somefield", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("someValue", metadata.get(Metadata.Key.of("somefield", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("entity", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("", metadata.get(Metadata.Key.of("entity", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    public void shouldThrowOperationNotSupportedExceptionWhenMappedHeaderValueIsComplexType() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("staticKey", "$GenericResponse.errors");
        this.protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                template
        );
        GenericResponse payload = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("detail_of_error")
                .addErrors(GenericError.newBuilder()
                        .setCode("404")
                        .setCause("not_found")
                        .setEntity("GTF")
                        .build())
                .build();

        Assertions.assertThrows(OperationNotSupportedException.class, () -> protoToMetadataMapper.buildGrpcMetadata(payload.toByteArray()));
    }

    @Test
    public void shouldBuildEmptyMetadataWhenConfigurationIsEmpty() {
        this.protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                new HashMap<>()
        );

        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(new byte[0]);

        Assertions.assertTrue(metadata.keys().isEmpty());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenConfigurationContainsUnregisteredExpression() {
        Map<String, String> template = new HashMap<>();
        template.put("$UnregisteredPayload.detail", "$GenericResponse.success");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + GenericResponse.errors[0].code)");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                template
        ));
    }
}
