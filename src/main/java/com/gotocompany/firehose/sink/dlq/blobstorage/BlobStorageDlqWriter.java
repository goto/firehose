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
import com.gotocompany.firehose.sink.dlq.DlqDateUtils;
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
        Map<Path, List<Message>> messagesByPartition = messages.stream()
                .collect(Collectors.groupingBy(this::createPartition));
        List<Message> failedMessages = new LinkedList<>();
        messagesByPartition.forEach((path, partitionedMessages) -> {
            String data = partitionedMessages.stream().map(this::convertToString).collect(Collectors.joining("\n"));
            String fileName = UUID.randomUUID().toString();
            String objectName = path.resolve(fileName).toString();
            String partitionDate = extractDateFromPath(path);
            try {
                blobStorage.store(objectName, data.getBytes(StandardCharsets.UTF_8));
                captureSuccessMetrics(partitionedMessages, partitionDate);
            } catch (BlobStorageException e) {
                log.warn("Failed to store into DLQ messages into blob storage", e);
                captureFailureMetrics(partitionedMessages, partitionDate);
                failedMessages.addAll(partitionedMessages);
            }
        });
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
            log.warn("Not able to convert message into json", e);
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
