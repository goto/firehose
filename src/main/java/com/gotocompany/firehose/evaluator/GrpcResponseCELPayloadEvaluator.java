package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class GrpcResponseCELPayloadEvaluator implements PayloadEvaluator<Message> {

    private final String celExpression;
    private Script script;
    private Descriptors.Descriptor descriptor;

    public GrpcResponseCELPayloadEvaluator(Descriptors.Descriptor descriptor, String celExpression) {
        this.celExpression = celExpression;
        this.descriptor = descriptor;
        this.script = buildScript(descriptor);
    }

    @Override
    public boolean evaluate(Message payload) {
        try {
            Map<String, Object> arguments = new HashMap<>();
            arguments.put(payload.getDescriptorForType().getFullName(), payload);
            return getScript(payload.getDescriptorForType()).execute(Boolean.class, arguments);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(
                    "Failed to evaluate payload with CEL Expression with reason: " + e.getMessage(), e);
        }
    }

    private Script getScript(Descriptors.Descriptor descriptor) throws ScriptCreateException {
        if (!descriptor.equals(this.descriptor)) {
            synchronized (this) {
                if (!descriptor.equals(this.descriptor)) {
                    this.script = buildScript(descriptor);
                    this.descriptor = descriptor;
                }
            }
        }
        return this.script;
    }

    private Script buildScript(Descriptors.Descriptor descriptor) {
        try {
            log.info("Building new CEL Script");
            return ScriptHost.newBuilder()
                    .build()
                    .buildScript(this.celExpression)
                    .withDeclarations(Decls.newVar(descriptor.getFullName(), Decls.newObjectType(descriptor.getFullName())))
                    .withTypes(DynamicMessage.newBuilder(descriptor).getDefaultInstanceForType())
                    .build();
        } catch (ScriptCreateException e) {
            throw new IllegalArgumentException("Failed to build CEL Script due to : " + e.getMessage(), e);
        }
    }
}