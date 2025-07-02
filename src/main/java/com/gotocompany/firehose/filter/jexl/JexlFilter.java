package com.gotocompany.firehose.filter.jexl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * A concrete class of Filter. This class is responsible
 * for filtering the messages based on a filter condition.
 * <p>
 * The filter expression is obtained from the {@link FilterConfig#getFilterJexlExpression()}
 * along with configurations for {@link FilterConfig#getFilterDataSource()} - [key|message]
 * and {@link FilterConfig#getFilterSchemaProtoClass()} - FQCN of the protobuf schema.
 */
public class JexlFilter implements Filter {

    private static final String METRIC_PREFIX = "firehose_jexl_filter_";
    private static final String DESERIALIZATION_ERRORS = METRIC_PREFIX + "deserialization_errors_total";

    private final Expression expression;
    private final FilterDataSourceType filterDataSourceType;
    private final String protoSchema;
    private final boolean dropDeserializationError;
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Message filter.
     *
     * @param filterConfig    the consumer config
     * @param firehoseInstrumentation the instrumentation
     */
    public JexlFilter(FilterConfig filterConfig, FirehoseInstrumentation firehoseInstrumentation) {
        JexlEngine engine = new JexlEngine();
        engine.setSilent(false);
        engine.setStrict(true);
        this.filterDataSourceType = filterConfig.getFilterDataSource();
        this.protoSchema = filterConfig.getFilterSchemaProtoClass();
        this.dropDeserializationError = filterConfig.getFilterDropDeserializationError();
        this.firehoseInstrumentation = firehoseInstrumentation;
        firehoseInstrumentation.logInfo("\n\tFilter type: {}", this.filterDataSourceType);
        this.expression = engine.createExpression(filterConfig.getFilterJexlExpression());
        firehoseInstrumentation.logInfo("\n\tFilter schema: {}", this.protoSchema);
        firehoseInstrumentation.logInfo("\n\tFilter expression: {}", filterConfig.getFilterJexlExpression());
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return {@link Message}
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        for (Message message : messages) {
            try {
                Object data = (filterDataSourceType.equals(FilterDataSourceType.KEY)) ? message.getLogKey() : message.getLogMessage();
                Object obj;
                try {
                    obj = MethodUtils.invokeStaticMethod(Class.forName(protoSchema), "parseFrom", data);
                } catch (InvocationTargetException e) {
                    if (dropDeserializationError && e.getCause() instanceof InvalidProtocolBufferException) {
                        firehoseInstrumentation.captureCount(DESERIALIZATION_ERRORS, 1L);
                        firehoseInstrumentation.logWarn("Failed to deserialize protobuf message: {}", e.getCause().getMessage());
                        filteredMessages.addToInvalidMessages(message);
                        continue;
                    } else {
                        throw new FilterException("Failed while filtering EsbMessages", e);
                    }
                }
                if (evaluate(obj)) {
                    filteredMessages.addToValidMessages(message);
                } else {
                    filteredMessages.addToInvalidMessages(message);
                }
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
                throw new FilterException("Failed while filtering EsbMessages", e);
            }
        }
        return filteredMessages;

    }

    private boolean evaluate(Object data) throws FilterException {
        Object result;
        try {
            result = expression.evaluate(convertDataToContext(data));
        } catch (JexlException | IllegalAccessException e) {
            throw new FilterException("Failed while filtering " + e.getMessage());
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            throw new FilterException("Expression should be correct!!");
        }
    }

    private JexlContext convertDataToContext(Object t) throws IllegalAccessException {
        JexlContext context = new MapContext();
        context.set(getObjectAccessor(), t);
        return context;
    }

    private String getObjectAccessor() {
        String[] schemaNameSplit = protoSchema.split("\\.");
        String objectAccessor = schemaNameSplit[schemaNameSplit.length - 1];
        return objectAccessor.substring(0, 1).toLowerCase() + objectAccessor.substring(1);
    }
}
