package com.gotocompany.firehose.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.error.ErrorScope;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.common.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gotocompany.firehose.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.RETRY_ATTEMPTS_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    /** Supplies the inter-attempt delay strategy. */
    private final BackOffProvider backOffProvider;
    /** Records retry counters, per-message metrics, and debug logs. */
    private final FirehoseInstrumentation firehoseInstrumentation;
    /** Supplies retry limits and input-schema settings. */
    private final AppConfig appConfig;
    /** Parses message bytes into protos for debug logging of retried messages. */
    private final KeyOrMessageParser parser;
    /** Splits failed messages by error scope to decide what is retryable. */
    private final ErrorHandler errorHandler;

    /**
     * Creates a retry decorator around the given sink.
     *
     * @param sink                    the wrapped sink to retry pushes against
     * @param backOffProvider         the strategy used to pause between attempts
     * @param firehoseInstrumentation the instrumentation used for retry metrics and logs
     * @param appConfig               the application config supplying retry limits
     * @param parser                  the parser used to render messages for debug logs
     * @param errorHandler            the handler that classifies which errors are retryable
     */
    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, FirehoseInstrumentation firehoseInstrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorHandler errorHandler) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.appConfig = appConfig;
        this.parser = parser;
        this.errorHandler = errorHandler;
    }

    /**
     * Pushes messages with retry.
     *
     * @param inputMessages list of messages
     * @return the remaining failed messages
     * @throws IOException           the io exception
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(failedMessages, ErrorScope.RETRY);
        List<Message> messagesAfterRetry = doRetry(splitLists.get(Boolean.TRUE));
        if (!messagesAfterRetry.isEmpty() && appConfig.getRetryFailAfterMaxAttemptsEnable()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }
        messagesAfterRetry.addAll(splitLists.get(Boolean.FALSE));
        return messagesAfterRetry;
    }

    /**
     * Logs the messages about to be retried, when debug logging is enabled.
     *
     * <p>Protobuf input is parsed into {@link DynamicMessage} form, while JSON input is rendered from
     * the message key or body depending on the configured parser mode.
     *
     * @param messageList the messages that will be retried
     * @throws IOException              if a protobuf message cannot be parsed
     * @throws IllegalArgumentException if the configured input schema type is not supported
     */
    private void logDebug(List<Message> messageList) throws IOException {
        if (firehoseInstrumentation.isDebugEnabled()) {
            switch (appConfig.getInputSchemaType()) {
                case PROTOBUF:
                    List<DynamicMessage> serializedBody = new ArrayList<>();
                    for (Message message : messageList) {
                        serializedBody.add(parser.parse(message));
                    }
                    firehoseInstrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
                    break;
                case JSON:
                    List<String> messages = messageList.stream().map(m -> {
                        if (appConfig.getKafkaRecordParserMode().equals("key")) {
                            return new String(m.getLogKey());
                        } else {
                            return new String(m.getLogMessage());
                        }
                    }).collect(Collectors.toList());
                    firehoseInstrumentation.logDebug("Retry failed messages: \n{}", messages.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected value: " + appConfig.getInputSchemaType());
            }
        }
    }

    /**
     * Pauses before the next retry attempt, unless there are no messages left to retry.
     *
     * @param messageList  the messages still pending retry
     * @param attemptCount the current attempt count, used to size the delay
     */
    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    /**
     * Repeatedly re-pushes the retryable messages until they succeed or attempts are exhausted.
     *
     * <p>Each round pushes the remaining messages, keeps only those whose errors are still in
     * {@link ErrorScope#RETRY}, backs off, and increments the attempt count. Total, success, and
     * failure message metrics are recorded around the loop.
     *
     * @param messages the retryable messages to attempt
     * @return the messages that are still failing after all attempts
     * @throws IOException if the wrapped sink fails during a retry push
     */
    private List<Message> doRetry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        firehoseInstrumentation.logInfo("Maximum retry attempts: {}", appConfig.getRetryMaxAttempts());
        retryMessages.forEach(m -> m.setDefaultErrorIfNotPresent());

        if (appConfig.getRetryMaxAttempts() > 0 || appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE) {
            retryMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1));
        }

        int attemptCount = 1;
        while ((attemptCount <= appConfig.getRetryMaxAttempts() && !retryMessages.isEmpty())
                || (appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            firehoseInstrumentation.incrementCounter(RETRY_ATTEMPTS_TOTAL);
            firehoseInstrumentation.logInfo("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());
            logDebug(retryMessages);
            retryMessages = errorHandler.split(super.pushMessage(retryMessages), ErrorScope.RETRY).get(Boolean.TRUE);
            backOff(retryMessages, attemptCount);
            attemptCount++;
        }

        if (appConfig.getRetryMaxAttempts() > 0 || appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE) {
            firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryMessages.size());
            retryMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        }
        return retryMessages;
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
