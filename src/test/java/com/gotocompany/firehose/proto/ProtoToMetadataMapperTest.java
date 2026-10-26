package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import com.gotocompany.firehose.consumer.TestUser;
import com.gotocompany.firehose.consumer.TestUserEmail;
import com.google.protobuf.Descriptors;
import io.grpc.Metadata;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProtoToMetadataMapperTest {

    private ProtoToMetadataMapper protoToMetadataMapper;

    @Before
    public void setup() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("someField", "someValue");
        template.put("$GenericResponse.success", "staticValue");
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
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("false", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("staticValue", metadata.get(Metadata.Key.of("false", Metadata.ASCII_STRING_MARSHALLER)));
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

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOperationNotSupportedExceptionWhenMappedHeaderValueIsComplexType() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("staticKey", "$GenericResponse.errors");

        new ProtoToMetadataMapper(GenericResponse.getDescriptor(), template);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenConfigurationContainsUnregisteredExpression() {
        Map<String, String> template = new HashMap<>();
        template.put("$UnregisteredPayload.detail", "$GenericResponse.success");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + GenericResponse.errors[0].code)");

        new ProtoToMetadataMapper(GenericResponse.getDescriptor(), template);
    }

    @Test
    public void shouldEvaluateCelFilterMacroConfiguredViaSinkGrpcMetadata() {
        ProtoToMetadataMapper mapper = mapperFromSinkGrpcMetadata(
                "error-cause:$(GenericResponse.errors.filter(e, e.code == \"404\")[0].cause),source:app");

        Metadata metadata = mapper.buildGrpcMetadata(responseWithErrors().toByteArray());

        Assertions.assertEquals(2, metadata.keys().size());
        Assertions.assertEquals("not_found", metadata.get(Metadata.Key.of("error-cause", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("app", metadata.get(Metadata.Key.of("source", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    public void shouldEvaluateCelExistsMacroWithTernaryConfiguredViaSinkGrpcMetadata() {
        ProtoToMetadataMapper mapper = mapperFromSinkGrpcMetadata(
                "has-not-found:$(GenericResponse.errors.exists(e, e.code == \"404\") ? \"yes\" : \"no\"),source:app");

        Metadata matching = mapper.buildGrpcMetadata(responseWithErrors().toByteArray());
        Metadata nonMatching = mapper.buildGrpcMetadata(GenericResponse.newBuilder().build().toByteArray());

        Assertions.assertEquals(2, matching.keys().size());
        Assertions.assertEquals("yes", matching.get(Metadata.Key.of("has-not-found", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("no", nonMatching.get(Metadata.Key.of("has-not-found", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("app", matching.get(Metadata.Key.of("source", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    public void shouldEvaluateCelFilterMacroOnPrimaryEmailConfiguredViaSinkGrpcMetadata() {
        ProtoToMetadataMapper mapper = mapperFromSinkGrpcMetadata(TestUser.getDescriptor(),
                "user-email:$(TestUser.emails.filter(e, e.primary)[0].address),source:app");
        TestUser user = TestUser.newBuilder()
                .setId("user-1")
                .addEmails(TestUserEmail.newBuilder().setAddress("secondary@example.com").setPrimary(false).build())
                .addEmails(TestUserEmail.newBuilder().setAddress("primary@example.com").setPrimary(true).build())
                .build();

        Metadata metadata = mapper.buildGrpcMetadata(user.toByteArray());

        Assertions.assertEquals(2, metadata.keys().size());
        Assertions.assertEquals("primary@example.com", metadata.get(Metadata.Key.of("user-email", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("app", metadata.get(Metadata.Key.of("source", Metadata.ASCII_STRING_MARSHALLER)));
    }

    private ProtoToMetadataMapper mapperFromSinkGrpcMetadata(String sinkGrpcMetadata) {
        return mapperFromSinkGrpcMetadata(GenericResponse.getDescriptor(), sinkGrpcMetadata);
    }

    private ProtoToMetadataMapper mapperFromSinkGrpcMetadata(Descriptors.Descriptor descriptor, String sinkGrpcMetadata) {
        GrpcSinkConfig grpcSinkConfig = ConfigFactory.create(GrpcSinkConfig.class,
                Collections.singletonMap("SINK_GRPC_METADATA", sinkGrpcMetadata));
        return new ProtoToMetadataMapper(descriptor, grpcSinkConfig.getSinkGrpcMetadata());
    }

    private GenericResponse responseWithErrors() {
        return GenericResponse.newBuilder()
                .addErrors(GenericError.newBuilder().setCode("500").setCause("internal").build())
                .addErrors(GenericError.newBuilder().setCode("404").setCause("not_found").build())
                .build();
    }

}
