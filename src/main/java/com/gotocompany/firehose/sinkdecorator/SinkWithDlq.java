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
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final DlqConfig dlqConfig;
    private final ErrorHandler errorHandler;

    private final FirehoseInstrumentation firehoseInstrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.dlqConfig = dlqConfig;
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param inputMessages list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
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

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

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

    private String calculateDateFromMessage(Message message) {
        return DlqDateUtils.getDateFromMessage(message, dlqConfig.getDlqBlobFilePartitionTimezone());
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
