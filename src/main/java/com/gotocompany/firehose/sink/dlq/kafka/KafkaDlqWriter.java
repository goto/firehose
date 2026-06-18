package com.gotocompany.firehose.sink.dlq.kafka;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link DlqWriter} that republishes undeliverable messages to a Kafka retry topic.
 * <p>
 * Each message's key, value and headers are sent to the configured topic via a Kafka producer. Sends
 * are asynchronous; the writer waits for all callbacks to complete and returns the messages whose
 * sends failed so they can be retried.
 *
 * @see DlqWriter
 * @see DlqWriterFactory
 */
public class KafkaDlqWriter implements DlqWriter {

    /** Producer used to publish messages to the retry topic. */
    private Producer<byte[], byte[]> kafkaProducer;
    /** The Kafka retry topic messages are republished to. */
    private final String topic;
    /** Instrumentation used for logging and error metrics. */
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a Kafka DLQ writer.
     *
     * @param kafkaProducer the producer used to publish to the retry topic
     * @param topic the Kafka retry topic to publish to
     * @param firehoseInstrumentation the instrumentation used for logging and metrics
     */
    public KafkaDlqWriter(Producer<byte[], byte[]> kafkaProducer, String topic, FirehoseInstrumentation firehoseInstrumentation) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Republishes the given messages to the Kafka retry topic.
     * <p>
     * Sends each message asynchronously, waits for every send to complete, and collects the ones that
     * failed. Returns immediately with an empty list when the batch is empty. If the wait is
     * interrupted the interruption is logged and recorded as a non-fatal error.
     *
     * @param messages the messages to republish
     * @return the messages that failed to be sent, empty if all succeeded
     * @throws IOException if a non-retryable error occurs while writing
     */
    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        if (messages.isEmpty()) {
            return Collections.emptyList();
        }
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger recordsProcessed = new AtomicInteger();
        List<Message> failedMessages = new ArrayList<>();

        firehoseInstrumentation.logInfo("Pushing {} messages to retry queue topic : {}", messages.size(), topic);
        for (Message message : messages) {
            kafkaProducer.send(new ProducerRecord<>(topic, null, null, message.getLogKey(), message.getLogMessage(),
                    message.getHeaders()), (metadata, e) -> {
                recordsProcessed.incrementAndGet();

                if (e != null) {
                    synchronized (failedMessages) {
                        failedMessages.add(message);
                    }
                }
                if (recordsProcessed.get() == messages.size()) {
                    completedLatch.countDown();
                }
            });
        }
        try {
            completedLatch.await();
        } catch (InterruptedException e) {
            firehoseInstrumentation.logWarn(e.getMessage());
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "");
        }
        firehoseInstrumentation.logInfo("Successfully pushed {} messages to {}", messages.size() - failedMessages.size(), topic);
        return failedMessages;
    }
}
