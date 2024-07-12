package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.OperationNotSupportedException;
import com.gotocompany.firehose.utils.CelUtils;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.grpc.Metadata;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtoToMetadataMapper {

    private static final Pattern CEL_EXPRESSION_MARKER = Pattern.compile("^\\$(.+)");
    private static final int EXACT_CEL_EXPRESSION_GROUP_INDEX = 1;

    private final Map<String, CelRuntime.Program> celExpressionToProgramMapper;
    private final Map<String, String> metadataTemplate;
    private final Descriptors.Descriptor descriptor;

    public ProtoToMetadataMapper(Descriptors.Descriptor descriptor, Map<String, String> metadataTemplate) {
        this.metadataTemplate = metadataTemplate;
        this.descriptor = descriptor;
        this.celExpressionToProgramMapper = initializeCelPrograms();
    }

    public Metadata buildGrpcMetadata(byte[] message) {
        try {
            if (MapUtils.isEmpty(metadataTemplate)) {
                return new Metadata();
            }
            return buildGrpcMetadata(DynamicMessage.parseFrom(descriptor, message));
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("Failed to parse protobuf message", e);
        }
    }

    private Metadata buildGrpcMetadata(Message message) {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, String> entry : metadataTemplate.entrySet()) {
            String updatedKey = evaluateExpression(entry.getKey(), message).toString();
            Object updatedValue = evaluateExpression(entry.getValue(), message);
            metadata.put(Metadata.Key.of(updatedKey.trim(), Metadata.ASCII_STRING_MARSHALLER), updatedValue.toString());
        }
        return metadata;
    }

    private Object evaluateExpression(String expression, Message message) {
        Matcher matcher = CEL_EXPRESSION_MARKER.matcher(expression);
        if (!matcher.find()) {
            return expression;
        }
        String celExpression = matcher.group(EXACT_CEL_EXPRESSION_GROUP_INDEX);
        return Optional.ofNullable(celExpressionToProgramMapper.get(celExpression))
                .map(program -> {
                    Object val = CelUtils.evaluate(program, message);
                    if (isComplexType(val)) {
                        throw new OperationNotSupportedException("Complex type is not supported");
                    }
                    return val;
                }).orElse(expression);
    }

    private boolean isComplexType(Object object) {
        return !(object instanceof String || object instanceof Number || object instanceof Boolean);
    }

    private CelCompiler initializeCelCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.values())
                .addVar(this.descriptor.getFullName(), StructTypeReference.create(this.descriptor.getFullName()))
                .addMessageTypes(this.descriptor)
                .build();
    }

    private Map<String, CelRuntime.Program> initializeCelPrograms() {
        CelRuntime celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
        CelCompiler celCompiler = initializeCelCompiler();
        return this.metadataTemplate.entrySet()
                .stream()
                .filter(entry -> Objects.nonNull(entry.getValue()))
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .map(string -> {
                    Matcher matcher = CEL_EXPRESSION_MARKER.matcher(string);
                    if (matcher.find()) {
                        return matcher.group(EXACT_CEL_EXPRESSION_GROUP_INDEX);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Function.identity(), celExpression -> initializeCelProgram(celExpression, celRuntime, celCompiler)));
    }

    private CelRuntime.Program initializeCelProgram(String celExpression, CelRuntime celRuntime, CelCompiler celCompiler) {
        try {
            CelAbstractSyntaxTree celAbstractSyntaxTree = celCompiler.compile(celExpression)
                    .getAst();
            return celRuntime.createProgram(celAbstractSyntaxTree);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException("Failed to create CEL program with expression : " + celExpression, e);
        }
    }

}
