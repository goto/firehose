package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
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

import java.io.IOException;
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
    private static final int CEL_EXPRESSION_GROUP_INDEX = 1;

    private final Map<String, CelRuntime.Program> celExpressionToProgramMapper;
    private final Map<String, Object> metadataTemplate;

    public ProtoToMetadataMapper(Descriptors.Descriptor descriptor, Map<String, Object> metadataTemplate) {
        this.metadataTemplate = metadataTemplate;
        this.celExpressionToProgramMapper = initializeCelPrograms(metadataTemplate, descriptor);
    }

    public Metadata buildGrpcMetadata(Message message) throws IOException {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, Object> entry : metadataTemplate.entrySet()) {
            String updatedKey = evaluateValue(entry.getKey(), message).toString();
            Object updatedValue = entry.getValue() instanceof String ? evaluateValue(entry.getValue().toString(), message) : entry.getValue();
            metadata.put(Metadata.Key.of(updatedKey.trim(), Metadata.ASCII_STRING_MARSHALLER), updatedValue.toString());
        }
        return metadata;
    }

    private Object evaluateValue(String key, Message message) {
        Matcher matcher = CEL_EXPRESSION_MARKER.matcher(key);
        if (!matcher.find()) {
            return key;
        }
        return Optional.ofNullable(celExpressionToProgramMapper.get(matcher.group(1)))
                .map(program -> {
                    Object val = CelUtils.evaluate(program, message);
                    if (isComplexType(val)) {
                        throw new OperationNotSupportedException("Complex type is not supported");
                    }
                    return val;
                }).orElse(key);
    }

    private boolean isComplexType(Object object) {
        return !(object instanceof String || object instanceof Number || object instanceof Boolean);
    }

    private CelCompiler initializeCelCompiler(Descriptors.Descriptor descriptor) {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.values())
                .addVar(descriptor.getFullName(), StructTypeReference.create(descriptor.getFullName()))
                .addMessageTypes(descriptor)
                .build();
    }

    private Map<String, CelRuntime.Program> initializeCelPrograms(Map<String, Object> metadataTemplate, Descriptors.Descriptor descriptor) {
        CelRuntime celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
        CelCompiler celCompiler = initializeCelCompiler(descriptor);
        return metadataTemplate.entrySet()
                .stream()
                .filter(entry -> entry.getValue() instanceof String)
                .flatMap(e -> Stream.of(e.getKey(), e.getValue().toString()))
                .map(keyword -> {
                    Matcher matcher = CEL_EXPRESSION_MARKER.matcher(keyword);
                    if (matcher.find()) {
                        return matcher.group(CEL_EXPRESSION_GROUP_INDEX);
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
