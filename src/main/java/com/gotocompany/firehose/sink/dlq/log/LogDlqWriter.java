package com.gotocompany.firehose.sink.dlq.log;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link DlqWriter} that writes undeliverable messages to the application log.
 * <p>
 * Logs each message's key, value and error stack trace at info level. As logging cannot fail a
 * message, {@link #write(List)} always reports that every message was handled.
 *
 * @see DlqWriter
 */
public class LogDlqWriter implements DlqWriter {
    /** Instrumentation used to log the dead-lettered messages. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a logging DLQ writer.
     *
     * @param firehoseInstrumentation the instrumentation used to log messages
     */
    public LogDlqWriter(FirehoseInstrumentation firehoseInstrumentation) {
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Logs each message's key, value and error to the application log.
     *
     * @param messages the messages to log
     * @return an empty list, since logging always succeeds
     * @throws IOException never thrown; declared to satisfy the {@link DlqWriter} contract
     */
    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        for (Message message : messages) {
            String key = message.getLogKey() == null ? "" : new String(message.getLogKey());
            String value = message.getLogMessage() == null ? "" : new String(message.getLogMessage());

            String error = "";
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo != null) {
                if (errorInfo.getException() != null) {
                    error = ExceptionUtils.getStackTrace(errorInfo.getException());
                }
            }

            firehoseInstrumentation.logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
        }
        return new LinkedList<>();
    }

}
