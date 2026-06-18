package com.gotocompany.firehose.metrics;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gotocompany.firehose.metrics.Metrics.ERROR_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.ERROR_TYPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.GLOBAL_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.MESSAGE_SCOPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.MESSAGE_TYPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.MessageType;
import static com.gotocompany.firehose.metrics.Metrics.PIPELINE_END_LATENCY_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.PIPELINE_EXECUTION_LIFETIME_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.SINK_PUSH_BATCH_SIZE_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.SINK_RESPONSE_TIME_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_FILTER_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL;

/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 */
public class FirehoseInstrumentation extends Instrumentation {

    /** Timestamp captured at the start of a sink execution, used to measure response time. */
    private Instant startExecutionTime;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public FirehoseInstrumentation(StatsDReporter statsDReporter, Logger logger) {
        super(statsDReporter, logger);
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
    public FirehoseInstrumentation(StatsDReporter statsDReporter, Class clazz) {
        super(statsDReporter, clazz);
    }

    /**
     * Gets start execution time.
     *
     * @return the start execution time
     */
    public Instant getStartExecutionTime() {
        return startExecutionTime;
    }
    // =================== LOGGING ===================

    // ============== FILTER MESSAGES ==============

    /**
     * Captures batch message histogram.
     *
     * @param pulledMessageCount the pulled message count
     */
    public void capturePulledMessageHistogram(long pulledMessageCount) {
        captureHistogram(SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL, pulledMessageCount);
    }

    /**
     * Captures filtered message count.
     *
     * @param filteredMessageCount the filtered message count
     */
    public void captureFilteredMessageCount(long filteredMessageCount) {
        captureCount(SOURCE_KAFKA_MESSAGES_FILTER_TOTAL, filteredMessageCount);
    }


    // ================ SinkExecutionTelemetry ================

    /**
     * Records the current time as the start of a sink execution.
     *
     * @return the captured start time
     */
    public Instant startExecution() {
        startExecutionTime = Instant.now();
        return startExecutionTime;
    }

    /**
     * Logs total messages executions.
     *
     * @param sinkType        the sink type
     * @param messageListSize the message list size
     */
    public void captureSinkExecutionTelemetry(String sinkType, Integer messageListSize) {
        logInfo("Processed {} messages in {}.", messageListSize, sinkType);
        captureDurationSince(SINK_RESPONSE_TIME_MILLISECONDS, this.startExecutionTime);
    }

    /**
     * @param totalMessages total messages
     */
    public void captureMessageBatchSize(long totalMessages) {
        captureHistogram(SINK_PUSH_BATCH_SIZE_TOTAL, totalMessages);
    }

    /**
     * Records an error-count metric for each of the given error types.
     *
     * @param errors the error types to count
     */
    public void captureErrorMetrics(List<ErrorType> errors) {
        errors.forEach(this::captureErrorMetrics);
    }

    /**
     * Records a single error-count metric for the given error type.
     *
     * @param errorType the error type to count
     */
    public void captureErrorMetrics(ErrorType errorType) {
        captureCount(ERROR_MESSAGES_TOTAL, 1L, String.format(ERROR_TYPE_TAG, errorType.name()));
    }

    // =================== Retry and DLQ Telemetry ======================

    /**
     * Records a message-count metric tagged by message type and, when present, error type.
     *
     * @param metric    the metric name to increment
     * @param type      the message type (total, success, or failure)
     * @param errorType the error type tag, or {@code null} to omit it
     * @param counter   the amount to add
     */
    public void captureMessageMetrics(String metric, MessageType type, ErrorType errorType, long counter) {
        if (errorType != null) {
            captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()), String.format(ERROR_TYPE_TAG, errorType.name()));
        } else {
            captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()));
        }
    }

    /**
     * Records a global message-count metric tagged with the given pipeline scope.
     *
     * @param scope   the pipeline stage the count is attributed to
     * @param counter the amount to add
     */
    public void captureGlobalMessageMetrics(Metrics.MessageScope scope, long counter) {
        captureCount(GLOBAL_MESSAGES_TOTAL, counter, String.format(MESSAGE_SCOPE_TAG, scope.name()));
    }

    /**
     * Records a message-count metric tagged by message type only.
     *
     * @param metric  the metric name to increment
     * @param type    the message type (total, success, or failure)
     * @param counter the amount to add
     */
    public void captureMessageMetrics(String metric, MessageType type, int counter) {
        captureMessageMetrics(metric, type, null, counter);
    }

    /**
     * Records a DLQ message-count metric for blob storage, tagged by type, error, and date.
     *
     * @param metric    the metric name to increment
     * @param type      the message type (total, success, or failure)
     * @param errorType the error type tag, or {@code null} to omit it
     * @param date      the partition date tag
     * @param counter   the amount to add
     */
    public void captureDLQBlobStorageMetrics(String metric, MessageType type, ErrorType errorType, String date, long counter) {
        if (errorType != null) {
            captureCount(metric, counter, String.format(Metrics.MESSAGE_TYPE_TAG, type.name()), String.format(Metrics.ERROR_TYPE_TAG, errorType.name()), String.format(Metrics.DLQ_DATE_TAG, date));
        } else {
            captureCount(metric, counter, String.format(Metrics.MESSAGE_TYPE_TAG, type.name()), String.format(Metrics.DLQ_DATE_TAG, date));
        }
    }

    /**
     * Records a non-fatal error for a message that could not be written to the DLQ.
     *
     * @param message the message that failed to be written
     * @param e       the failure that occurred
     */
    public void captureDLQErrors(Message message, Exception e) {
        captureNonFatalError("firehose_error_event", e, "Unable to send record with key {} and message {} to DLQ", message.getLogKey(), message.getLogMessage());
    }

    // ===================== Latency / LifetimeTillSink =====================

    /**
     * Records pipeline latency metrics for each message before it is pushed to the sink.
     *
     * <p>For every message it captures the time since the source event timestamp and the time since
     * the message was consumed.
     *
     * @param messages the messages whose latencies are recorded
     */
    public void capturePreExecutionLatencies(List<Message> messages) {
        messages.forEach(message -> {
            captureDurationSince(PIPELINE_END_LATENCY_MILLISECONDS, Instant.ofEpochMilli(message.getTimestamp()));
            captureDurationSince(PIPELINE_EXECUTION_LIFETIME_MILLISECONDS, Instant.ofEpochMilli(message.getConsumeTimestamp()));
        });
    }

    /**
     * Records a back-off sleep duration as a gauge-style value.
     *
     * @param metric    the metric name to record under
     * @param sleepTime the sleep duration in milliseconds
     */
    public void captureSleepTime(String metric, int sleepTime) {
        captureValue(metric, sleepTime);
    }

    // ===================== closing =================

    /**
     * Closes the underlying instrumentation and its StatsD resources.
     *
     * @throws IOException if the underlying resources fail to close
     */
    public void close() throws IOException {
        super.close();
    }
}
