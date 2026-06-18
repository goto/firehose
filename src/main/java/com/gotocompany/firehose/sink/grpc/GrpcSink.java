package com.gotocompany.firehose.sink.grpc;


import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.evaluator.PayloadEvaluator;
import com.gotocompany.firehose.exception.DefaultException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gotocompany.firehose.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    /** Client used to perform the unary gRPC calls. */
    private final GrpcClient grpcClient;
    /** Stencil client used to resolve protobuf schemas; closed on shutdown. */
    private final StencilClient stencilClient;
    /** Bound gRPC sink configuration. */
    private final GrpcSinkConfig grpcSinkConfig;
    /** Source messages for the current batch, set during preparation. */
    private List<Message> messages;
    /** Evaluates a gRPC response to decide whether a failed message is retryable. */
    private PayloadEvaluator<com.google.protobuf.Message> retryEvaluator;

    /**
     * Creates a gRPC sink with its client, schema resolver, configuration and retry policy.
     *
     * @param firehoseInstrumentation instrumentation used to emit metrics and logs
     * @param grpcClient              client used to perform the gRPC calls
     * @param stencilClient           Stencil client closed when the sink is closed
     * @param grpcSinkConfig          the bound gRPC sink configuration
     * @param retryEvaluator          evaluator deciding whether a failed response is retryable
     */
    public GrpcSink(FirehoseInstrumentation firehoseInstrumentation,
                    GrpcClient grpcClient,
                    StencilClient stencilClient,
                    GrpcSinkConfig grpcSinkConfig,
                    PayloadEvaluator<com.google.protobuf.Message> retryEvaluator) {
        super(firehoseInstrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
        this.grpcSinkConfig = grpcSinkConfig;
        this.retryEvaluator = retryEvaluator;
    }

    /**
     * Sends every message in the batch and returns those that failed.
     *
     * <p>Each message is dispatched through the gRPC client; a response whose {@code success} field is not
     * {@code true} causes the message to be collected as failed with retryable or non-retryable error info.
     *
     * @return the messages that failed and may need to be retried
     * @throws Exception if a gRPC call fails unexpectedly
     */
    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> failedMessages = new ArrayList<>();

        for (Message message : this.messages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            getFirehoseInstrumentation().logDebug("Response: {}", response);
            Object m = response.getField(response.getDescriptorForType().findFieldByName("success"));
            boolean success = (m != null) ? Boolean.valueOf(String.valueOf(m)) : false;

            if (!success) {
                getFirehoseInstrumentation().logWarn("Grpc Service returned error");
                failedMessages.add(message);
                setRetryableErrorInfo(message, response);
            }
        }
        getFirehoseInstrumentation().logDebug("Failed messages count: {}", failedMessages.size());
        return failedMessages;
    }

    /**
     * Records the batch of messages to be sent on the next execution.
     *
     * @param messages2 the batch of messages about to be delivered
     * @throws DeserializerException if a message cannot be deserialized
     */
    @Override
    protected void prepare(List<Message> messages2) throws DeserializerException {
        this.messages = messages2;
    }

    /**
     * Releases resources by discarding pending messages and closing the Stencil client.
     *
     * @throws IOException if closing the Stencil client fails
     */
    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("GRPC connection closing");
        this.messages = new ArrayList<>();
        stencilClient.close();
    }

    /**
     * Attaches retryable or non-retryable error information to a failed message based on the response.
     *
     * @param message        the failed message to annotate
     * @param dynamicMessage the gRPC response evaluated to decide retryability
     */
    private void setRetryableErrorInfo(Message message, DynamicMessage dynamicMessage) {
        boolean eligibleToRetry = retryEvaluator.evaluate(dynamicMessage);
        if (eligibleToRetry) {
            getFirehoseInstrumentation().logDebug("Retrying grpc service");
            message.setErrorInfo(new ErrorInfo(new DefaultException("Retryable gRPC Error"), grpcSinkConfig.getSinkGrpcRetryErrorType()));
            return;
        }
        message.setErrorInfo(new ErrorInfo(new DefaultException("Non Retryable gRPC Error"), ErrorType.SINK_NON_RETRYABLE_ERROR));
    }
}
