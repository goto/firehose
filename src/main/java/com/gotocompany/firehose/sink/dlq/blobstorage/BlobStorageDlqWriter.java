package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@link DlqWriter} that stores undeliverable messages in blob storage as newline-delimited JSON.
 * <p>
 * Messages are grouped into partitions by topic and date (the date is derived from either the produce
 * or consume timestamp according to configuration), serialized to JSON {@link DlqMessage} records, and
 * uploaded as one object per partition under a random file name. Partitions that fail to upload have
 * all their messages returned as failed; success and failure metrics are recorded per partition and
 * error type.
 *
 * @see DlqWriter
 * @see DlqMessage
 * @see DlqDateUtils
 */
@Slf4j
public class BlobStorageDlqWriter implements DlqWriter {
    /** Threshold in megabytes above which a written batch is logged as unusually large. */
    private static final int LARGE_BATCH_THRESHOLD_MB = 10;
    /** Number of bytes in a kilobyte, used to compute the large-batch threshold. */
    private static final int BYTES_PER_KB = 1024;
    /** Number of kilobytes in a megabyte, used to compute the large-batch threshold. */
    private static final int KB_PER_MB = 1024;

    /** The blob storage backend DLQ files are written to. */
    private final BlobStorage blobStorage;
    /** Jackson mapper used to serialize messages to JSON. */
    private final ObjectMapper objectMapper;
    /** Dead letter queue configuration controlling partitioning. */
    private final DlqConfig dlqConfig;
    /** Instrumentation used to log and emit DLQ metrics. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a blob storage DLQ writer.
     *
     * @param blobStorage the blob storage backend to write to
     * @param dlqConfig the dead letter queue configuration controlling partitioning
     * @param firehoseInstrumentation the instrumentation used for logging and metrics
     */
    public BlobStorageDlqWriter(BlobStorage blobStorage, DlqConfig dlqConfig, FirehoseInstrumentation firehoseInstrumentation) {
        this.blobStorage = blobStorage;
        this.objectMapper = new ObjectMapper();
        this.dlqConfig = dlqConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Writes the given messages to blob storage, grouped into per-topic, per-date partitions.
     * <p>
     * Each partition's messages are serialized to newline-delimited JSON and uploaded as a single
     * object. Messages that cannot be serialized are skipped; if a partition's upload fails all of its
     * messages are returned as failed. Returns immediately when the batch is empty.
     *
     * @param messages the messages to write to the dead letter queue
     * @return the messages whose partitions failed to upload, empty if all succeeded
     * @throws IOException if a non-retryable error occurs while writing
     */
    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        if (messages.isEmpty()) {
            return messages;
        }

        firehoseInstrumentation.logInfo("Starting DLQ blob storage write for {} messages", messages.size());
        firehoseInstrumentation.logDebug("DLQ blob storage partition key type: {}, timezone: {}",
            dlqConfig.getDlqBlobFilePartitionKey(), dlqConfig.getDlqBlobFilePartitionTimezone());

        Map<Path, List<Message>> messagesByPartition = messages.stream()
                .collect(Collectors.groupingBy(this::createPartition));

        if (log.isDebugEnabled()) {
            messagesByPartition.forEach((path, partitionedMessages) -> {
                String partitionDate = extractDateFromPath(path);
                log.debug("Partition {} has {} messages", partitionDate, partitionedMessages.size());
                partitionedMessages.forEach(msg ->
                    log.debug("Message - topic: {}, partition: {}, offset: {}, errorType: {}",
                        msg.getTopic(), msg.getPartition(), msg.getOffset(),
                        msg.getErrorInfo() != null ? msg.getErrorInfo().getErrorType() : "UNKNOWN"));
            });

            Map<String, Long> errorDistribution = messages.stream()
                .filter(m -> m.getErrorInfo() != null)
                .collect(Collectors.groupingBy(
                    m -> m.getErrorInfo().getErrorType().name(),
                    Collectors.counting()));

            log.debug("Batch error distribution: {}", errorDistribution);
        }

        List<Message> failedMessages = new LinkedList<>();
        int successfulPartitions = 0;
        int failedPartitions = 0;

        for (Map.Entry<Path, List<Message>> entry : messagesByPartition.entrySet()) {
            Path path = entry.getKey();
            List<Message> partitionedMessages = entry.getValue();

            int[] serializationFailures = {0};
            String data = partitionedMessages.stream()
                .map(msg -> {
                    String json = convertToString(msg);
                    if (json.isEmpty()) {
                        serializationFailures[0]++;
                    }
                    return json;
                })
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("\n"));

            if (serializationFailures[0] > 0) {
                log.warn("JSON serialization failed for {} messages in partition {}",
                    serializationFailures[0], extractDateFromPath(path));
            }

            String fileName = UUID.randomUUID().toString();
            String objectName = path.resolve(fileName).toString();
            String partitionDate = extractDateFromPath(path);

            if (objectName.contains("//") || objectName.contains("\\")) {
                log.warn("Potentially invalid object path detected: {}", objectName);
            }

            log.debug("Created DLQ object path - topic: {}, partition: {}, date: {}, object: {}",
                partitionedMessages.get(0).getTopic(), partitionedMessages.get(0).getPartition(),
                partitionDate, objectName);

            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

            if (dataBytes.length == 0) {
                log.warn("Empty DLQ batch detected for partition {}, objectName: {}", partitionDate, objectName);
            }

            if (dataBytes.length > LARGE_BATCH_THRESHOLD_MB * BYTES_PER_KB * KB_PER_MB) {
                log.warn("Large DLQ batch detected - partition: {}, object: {}, size: {} bytes ({} messages)",
                    partitionDate, objectName, dataBytes.length, partitionedMessages.size());
            }

            log.debug("Writing {} messages to blob storage partition {}, object: {}, size: {} bytes",
                partitionedMessages.size(), partitionDate, objectName, dataBytes.length);

            try {
                long startTime = System.currentTimeMillis();
                blobStorage.store(objectName, dataBytes);
                long duration = System.currentTimeMillis() - startTime;

                if (log.isDebugEnabled()) {
                    log.debug("Successfully stored object to blob storage: {} ({} messages, {} bytes, {}ms)",
                        objectName, partitionedMessages.size(), dataBytes.length, duration);
                }
                captureSuccessMetrics(partitionedMessages, partitionDate);
                successfulPartitions++;
            } catch (BlobStorageException e) {
                log.warn("Failed to store DLQ messages into blob storage - object: {}, partition: {}, messages: {}, errorType: {}, errorMessage: {}",
                    objectName, partitionDate, partitionedMessages.size(), e.getErrorType(), e.getMessage(), e);
                captureFailureMetrics(partitionedMessages, partitionDate);
                failedMessages.addAll(partitionedMessages);
                failedPartitions++;
            }
        }

        firehoseInstrumentation.logInfo("DLQ blob storage write complete - total: {}, successful partitions: {}, failed partitions: {}, successful messages: {}, failed messages: {}",
            messages.size(), successfulPartitions, failedPartitions, messages.size() - failedMessages.size(), failedMessages.size());

        if (!failedMessages.isEmpty() && log.isDebugEnabled()) {
            Map<String, List<Message>> failedByTopic = failedMessages.stream()
                .collect(Collectors.groupingBy(Message::getTopic));

            failedByTopic.forEach((topic, msgs) -> {
                long minOffset = msgs.stream().mapToLong(Message::getOffset).min().orElse(-1);
                long maxOffset = msgs.stream().mapToLong(Message::getOffset).max().orElse(-1);
                log.debug("Failed messages for topic {} - count: {}, offsetRange: {}-{}",
                    topic, msgs.size(), minOffset, maxOffset);
            });
        }

        return failedMessages;
    }

    /**
     * Serializes a message to its JSON {@link DlqMessage} representation.
     * <p>
     * The key and value are Base64-encoded and any error information is included. Returns an empty
     * string if serialization fails.
     *
     * @param message the message to serialize
     * @return the JSON representation, or an empty string if serialization failed
     */
    private String convertToString(Message message) {
        try {
            String errorString = "";
            String errorType = "";
            if (message.getErrorInfo() != null) {
                errorString = message.getErrorInfo().toString();
                errorType = message.getErrorInfo().getErrorType().name();
            }

            return objectMapper.writeValueAsString(new DlqMessage(
                    Base64.getEncoder()
                            .encodeToString(message.getLogKey() == null ? "".getBytes() : message.getLogKey()),
                    Base64.getEncoder()
                            .encodeToString(message.getLogMessage() == null ? "".getBytes() : message.getLogMessage()),
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getTimestamp(),
                    errorString,
                    errorType));
        } catch (JsonProcessingException e) {
            log.warn("Failed to convert message to JSON - topic: {}, partition: {}, offset: {}",
                message.getTopic(), message.getPartition(), message.getOffset(), e);
            return "";
        }
    }

    /**
     * Computes the blob storage partition path for a message.
     * <p>
     * The path is the message topic followed by a date derived from the configured partition key type
     * and timezone.
     *
     * @param message the message to partition
     * @return the partition path (topic and date) for the message
     */
    private Path createPartition(Message message) {
        DlqPartitionKeyType partitionKeyType = dlqConfig.getDlqBlobFilePartitionKey();
        firehoseInstrumentation.logDebug("DLQ partitioning message - topic: {}, partition: {}, offset: {}, produceTimestamp: {}, consumeTimestamp: {}",
                message.getTopic(), message.getPartition(), message.getOffset(), message.getTimestamp(), message.getConsumeTimestamp());
        String partitionDate = DlqDateUtils.getDateFromMessage(
                message,
                dlqConfig.getDlqBlobFilePartitionTimezone(),
                partitionKeyType);
        return Paths.get(message.getTopic(), partitionDate);
    }

    /**
     * Returns the date component, the final path segment, of a partition path.
     *
     * @param path the partition path
     * @return the date segment as a string
     */
    private String extractDateFromPath(Path path) {
        return path.getFileName().toString();
    }

    /**
     * Records DLQ success metrics for a partition's messages.
     *
     * @param messages the successfully written messages
     * @param date the partition date
     */
    private void captureSuccessMetrics(List<Message> messages, String date) {
        firehoseInstrumentation.captureDLQBlobStorageMetrics(
                Metrics.DLQ_MESSAGES_TOTAL,
                Metrics.MessageType.SUCCESS,
                null,
                date,
                messages.size()
        );
    }

    /**
     * Records DLQ failure metrics, by error type, for a partition's messages.
     *
     * @param messages the messages that failed to be written
     * @param date the partition date
     */
    private void captureFailureMetrics(List<Message> messages, String date) {
        messages.forEach(message -> {
            if (message.getErrorInfo() != null) {
                firehoseInstrumentation.captureDLQBlobStorageMetrics(
                        Metrics.DLQ_MESSAGES_TOTAL,
                        Metrics.MessageType.FAILURE,
                        message.getErrorInfo().getErrorType(),
                        date,
                        1
                );
            }
        });
    }

}
