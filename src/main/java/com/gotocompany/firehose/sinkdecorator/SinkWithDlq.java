package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.error.ErrorScope;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import com.gotocompany.firehose.sink.dlq.blobstorage.BlobStorageDlqWriter;
import com.gotocompany.firehose.sink.dlq.blobstorage.DlqDateUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.gotocompany.firehose.metrics.Metrics.DLQ_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.DLQ_RETRY_ATTEMPTS_TOTAL;

/**
 * {@link Sink} decorator that routes undeliverable messages to a dead-letter queue (DLQ).
 *
 * <p>After delegating a push, the failed messages are split by the {@link ErrorHandler} into those
 * whose errors are in {@link ErrorScope#DLQ} and the rest. DLQ-eligible messages are written through
 * a {@link DlqWriter} (Kafka, log, or blob storage), retried up to {@code DLQ_RETRY_MAX_ATTEMPTS}
 * with a back-off between attempts, and instrumented with DLQ counters. If writes are exhausted and
 * {@code DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE} is set, an {@link IOException} is thrown to fail
 * the consumer; otherwise the remaining failures plus the non-DLQ messages are returned. When the
 * underlying sink manages its own offsets, the processed messages are marked committable.
 */
public class SinkWithDlq extends SinkDecorator {

    /** Batch key used to group messages handled by the DLQ for offset tracking. */
    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    /** Writer that persists messages to the configured dead-letter destination. */
    private final DlqWriter writer;
    /** Supplies the delay between DLQ write attempts. */
    private final BackOffProvider backOffProvider;
    /** Supplies DLQ retry limits and partitioning settings. */
    private final DlqConfig dlqConfig;
    /** Splits failed messages by error scope to decide what is DLQ-eligible. */
    private final ErrorHandler errorHandler;

    /** Records DLQ counters, per-message errors, and logs. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a DLQ decorator around the given sink.
     *
     * @param sink                    the wrapped sink whose failures may be sent to the DLQ
     * @param writer                  the writer that persists messages to the DLQ
     * @param backOffProvider         the strategy used to pause between DLQ write attempts
     * @param dlqConfig               the DLQ configuration (retry limits, partitioning)
     * @param errorHandler            the handler that classifies which errors are DLQ-eligible
     * @param firehoseInstrumentation the instrumentation used for DLQ metrics and logs
     */
    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.dlqConfig = dlqConfig;
    }

    /**
     * Pushes messages and writes the DLQ-eligible failures to the dead-letter queue.
     *
     * <p>Failures are split into DLQ-eligible and non-eligible sets. Eligible messages are written
     * (with retries); if any remain and fail-on-max is enabled an exception is thrown. When the
     * underlying sink manages offsets, the processed messages are marked committable. The returned
     * list contains any messages that still failed plus the non-eligible ones.
     *
     * @param inputMessages the messages to push
     * @return the messages that could not be sent to the DLQ plus the non-DLQ failures
     * @throws IOException           if the wrapped sink fails, or DLQ writes are exhausted with fail-on-max enabled
     * @throws DeserializerException if the wrapped sink fails to deserialize a message
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        if (messages.isEmpty()) {
            return messages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(messages, ErrorScope.DLQ);
        List<Message> dlqEligibleMessages = splitLists.get(Boolean.TRUE);
        List<Message> filteredMessages = splitLists.get(Boolean.FALSE);

        firehoseInstrumentation.logDebug("DLQ eligibility split - eligible: {}, filtered: {}",
            dlqEligibleMessages != null ? dlqEligibleMessages.size() : 0,
            filteredMessages != null ? filteredMessages.size() : 0);

        List<Message> returnedMessages = doDLQ(dlqEligibleMessages);
        if (!returnedMessages.isEmpty() && dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()) {
            firehoseInstrumentation.logWarn("Exhausted maximum DLQ retry attempts - failing {} messages", returnedMessages.size());
            throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
        }
        if (super.canManageOffsets()) {
            super.addOffsetsAndSetCommittable(messages);
        }
        returnedMessages.addAll(filteredMessages);
        return returnedMessages;
    }

    /**
     * Pauses before the next DLQ write attempt, unless there are no messages left to write.
     *
     * @param messageList  the messages still pending a DLQ write
     * @param attemptCount the current attempt count, used to size the delay
     */
    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    /**
     * Writes the given messages to the DLQ, retrying failed writes up to the configured maximum.
     *
     * <p>Records total, success, and failure DLQ metrics (using blob-storage-specific metrics when
     * the writer targets blob storage) and captures per-message DLQ errors. Returns the messages that
     * still could not be written after all attempts.
     *
     * @param messages the DLQ-eligible messages to write (may be {@code null} or empty)
     * @return the messages that remain unwritten after all attempts
     * @throws IOException if the DLQ writer fails irrecoverably
     */
    private List<Message> doDLQ(List<Message> messages) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return new LinkedList<>();
        }

        List<Message> retryQueueMessages = new LinkedList<>(messages);
        boolean isBlobStorageDlq = writer instanceof BlobStorageDlqWriter;
        String writerType = isBlobStorageDlq ? "BLOB_STORAGE" : "KAFKA/LOG";

        firehoseInstrumentation.logInfo("Starting DLQ processing for {} messages using {} writer", messages.size(), writerType);

        retryQueueMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            if (isBlobStorageDlq) {
                String date = calculateDateFromMessage(m);
                firehoseInstrumentation.captureDLQBlobStorageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), date, 1);
            } else {
                firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
            }
        });

        if (isBlobStorageDlq && firehoseInstrumentation.isDebugEnabled()) {
            Map<String, Long> dateDistribution = retryQueueMessages.stream()
                .collect(Collectors.groupingBy(this::calculateDateFromMessage, Collectors.counting()));
            StringBuilder distribution = new StringBuilder("Message distribution by date - ");
            dateDistribution.forEach((date, count) -> distribution.append(date).append(": ").append(count).append(" messages, "));
            firehoseInstrumentation.logDebug(distribution.toString());
        }

        int attemptCount = 1;
        int maxAttempts = this.dlqConfig.getDlqRetryMaxAttempts();

        while (attemptCount <= maxAttempts && !retryQueueMessages.isEmpty()) {
            firehoseInstrumentation.logInfo("DLQ write attempt {}/{} for {} messages", attemptCount, maxAttempts, retryQueueMessages.size());
            firehoseInstrumentation.incrementCounter(DLQ_RETRY_ATTEMPTS_TOTAL);

            retryQueueMessages = writer.write(retryQueueMessages);

            retryQueueMessages.forEach(message -> Optional.ofNullable(message.getErrorInfo())
                    .flatMap(errorInfo -> Optional.ofNullable(errorInfo.getException()))
                    .ifPresent(e -> firehoseInstrumentation.captureDLQErrors(message, e)));

            if (!retryQueueMessages.isEmpty() && attemptCount < maxAttempts) {
                firehoseInstrumentation.logWarn("DLQ write attempt {}/{} failed for {} messages, will retry after backoff",
                    attemptCount, maxAttempts, retryQueueMessages.size());
            }

            backOff(retryQueueMessages, attemptCount);
            attemptCount++;
        }

        int successCount = messages.size() - retryQueueMessages.size();
        int failureCount = retryQueueMessages.size();

        if (!retryQueueMessages.isEmpty()) {
            Map<String, Long> errorTypeDistribution = retryQueueMessages.stream()
                .filter(m -> m.getErrorInfo() != null)
                .collect(Collectors.groupingBy(
                    m -> m.getErrorInfo().getErrorType().name(),
                    Collectors.counting()));

            firehoseInstrumentation.logInfo("Failed to process {} DLQ messages after {} attempts. Error distribution: {}",
                failureCount, maxAttempts, errorTypeDistribution);
        }

        firehoseInstrumentation.logInfo("DLQ processing complete - total: {}, successful: {}, failed: {}",
            messages.size(), successCount, failureCount);

        firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, successCount);
        retryQueueMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, successCount);
        return retryQueueMessages;
    }

    /**
     * Derives the partition date for a message, used to bucket blob-storage DLQ metrics and paths.
     *
     * @param message the message to derive the date from
     * @return the formatted date string in the configured DLQ partition timezone
     */
    private String calculateDateFromMessage(Message message) {
        return DlqDateUtils.getDateFromMessage(message, dlqConfig.getDlqBlobFilePartitionTimezone());
    }

    /**
     * Closes the wrapped sink.
     *
     * @throws IOException if the wrapped sink fails to close
     */
    @Override
    public void close() throws IOException {
        super.close();
    }
}
