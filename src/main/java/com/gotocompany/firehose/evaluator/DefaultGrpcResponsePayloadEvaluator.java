package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Message;

public class DefaultGrpcResponsePayloadEvaluator implements PayloadEvaluator<Message> {
    @Override
    public boolean evaluate(Message payload) {
        return true;
    }
}