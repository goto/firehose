package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Message;
import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import com.gotocompany.firehose.consumer.TestMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class GrpcResponseCelPayloadEvaluatorTest {

    private static final String CEL_EXPRESSION = "GenericResponse.success == false && GenericResponse.errors.exists(e, e.code == \"400\")";
    private PayloadEvaluator<Message> grpcPayloadEvaluator;

    @Before
    public void setup() {
        this.grpcPayloadEvaluator = new GrpcResponseCelPayloadEvaluator(GenericResponse.getDescriptor(), CEL_EXPRESSION);
    }

    @Test
    public void shouldEvaluateResponseToTrueWhenCELExpressionMatchesPayload() {
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("400")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = grpcPayloadEvaluator.evaluate(genericResponse);

        Assertions.assertTrue(result);
    }

    @Test
    public void shouldEvaluateResponseToFalseWhenCELExpressionDoesntMatchPayload() {
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("50000")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = grpcPayloadEvaluator.evaluate(genericResponse);

        Assertions.assertFalse(result);
    }

    @Test
    public void shouldEvaluateResponseToTrueWhenCelExpressionMatchesRange() {
        PayloadEvaluator<Message> evaluator = new GrpcResponseCelPayloadEvaluator(
                GenericResponse.getDescriptor(),
                "GenericResponse.errors.exists(e, int(e.code) >= 1000 && int(e.code) <= 2000)"
        );
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("1500")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = evaluator.evaluate(genericResponse);

        Assertions.assertTrue(result);
    }

    @Test
    public void shouldEvaluateResponseToFalseWhenCelExpressionMatchesRangeAndNotInSet() {
        PayloadEvaluator<Message> evaluator = new GrpcResponseCelPayloadEvaluator(
                GenericResponse.getDescriptor(),
                "GenericResponse.errors.exists(e, int(e.code) >= 1000 && int(e.code) <= 2000 && !(int(e.code) in [1500, 1600]))"
        );
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("1500")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = evaluator.evaluate(genericResponse);

        Assertions.assertFalse(result);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenCelValidationFailed() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new GrpcResponseCelPayloadEvaluator(
                GenericResponse.getDescriptor(), "GenericResponse.nonExistField == true"));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenPayloadIsNotRecognizedByDescriptor() {
        TestMessage unregisteredPayload = TestMessage.newBuilder()
                .setOrderUrl("url")
                .build();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> grpcPayloadEvaluator.evaluate(unregisteredPayload));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenCelExpressionContainsUnregisteredMacro() {
        String expressionWithUnregisteredMacro = "GenericResponse.errors.nonStandardMacro(e, e.code == \"400\")";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new GrpcResponseCelPayloadEvaluator(GenericResponse.getDescriptor(), expressionWithUnregisteredMacro));
    }

    @Test
    public void shouldThrowOperationNotSupportedExceptionWhenCelExpressionResultIsNotBoolean() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new GrpcResponseCelPayloadEvaluator(GenericResponse.getDescriptor(), "GenericResponse.errors"));
    }

}
