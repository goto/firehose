package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
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
        Map<String, Object> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + GenericResponse.errors[0].code)");
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
                .addErrors(GenericError.newBuilder()
                        .setCode("404")
                        .setCause("not_found")
                        .setEntity("GTF")
                        .build())
                .build();

        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(payload.toByteArray());

        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("false", metadata.get(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("not_found404", metadata.get(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
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
}
