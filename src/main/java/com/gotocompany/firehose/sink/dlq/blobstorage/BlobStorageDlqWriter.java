package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
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

@Slf4j
public class BlobStorageDlqWriter implements DlqWriter {
    private static final int LARGE_BATCH_THRESHOLD_MB = 10;
    private static final int BYTES_PER_KB = 1024;
    private static final int KB_PER_MB = 1024;

    private final BlobStorage blobStorage;
    private final ObjectMapper objectMapper;
    private final DlqConfig dlqConfig;
    private final FirehoseInstrumentation firehoseInstrumentation;

    public BlobStorageDlqWriter(BlobStorage blobStorage, DlqConfig dlqConfig, FirehoseInstrumentation firehoseInstrumentation) {
        this.blobStorage = blobStorage;
        this.objectMapper = new ObjectMapper();
        this.dlqConfig = dlqConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        if (messages.isEmpty()) {
            return messages;
        }

        firehoseInstrumentation.logInfo("Starting DLQ blob storage write for {} messages", messages.size());

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

    private Path createPartition(Message message) {
        String consumeDate = DlqDateUtils.getDateFromMessage(message, dlqConfig.getDlqBlobFilePartitionTimezone());
        return Paths.get(message.getTopic(), consumeDate);
    }

    private String extractDateFromPath(Path path) {
        return path.getFileName().toString();
    }

    private void captureSuccessMetrics(List<Message> messages, String date) {
        firehoseInstrumentation.captureDLQBlobStorageMetrics(
                Metrics.DLQ_MESSAGES_TOTAL,
                Metrics.MessageType.SUCCESS,
                null,
                date,
                messages.size()
        );
    }

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
