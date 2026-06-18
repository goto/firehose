package com.gotocompany.firehose.tracer;

import com.gotocompany.firehose.message.Message;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.tag.Tags;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sink Tracer.
 */
@AllArgsConstructor
@Getter
public class SinkTracer implements Traceable, Closeable {
    /** Underlying OpenTracing tracer used to build and report spans. */
    private Tracer tracer;
    /** Span operation name and component tag, derived from the sink type. */
    private String name;
    /** Whether tracing is active; when false, tracing methods are no-ops. */
    private boolean enabled;

    /**
     * Starts a span for each message when tracing is enabled.
     *
     * @param messages the batch of messages to trace
     * @return a span per message, or an empty list when tracing is disabled
     */
    @Override
    public List<Span> startTrace(List<Message> messages) {
        if (enabled) {
            return messages.stream().map(m -> traceMessage(m)).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Builds and starts a single consumer span for a message.
     *
     * <p>If the record headers carry a parent span context, the new span is linked to it with a
     * {@code FOLLOWS_FROM} reference.
     *
     * @param message the message to trace
     * @return the started span
     */
    private Span traceMessage(Message message) {
        SpanContext parentContext = null;
        if (message.getHeaders() != null) {
            parentContext = TracingKafkaUtils.extractSpanContext(message.getHeaders(), tracer);
        }

        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(name)
                .withTag(Tags.COMPONENT, name)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }
        return spanBuilder.start();

    }

    /**
     * Closes the underlying tracer.
     */
    @Override
    public void close() {
        tracer.close();
    }
}
