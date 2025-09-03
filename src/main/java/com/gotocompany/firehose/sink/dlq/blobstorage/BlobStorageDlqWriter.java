package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRulesException;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class BlobStorageDlqWriter implements DlqWriter {
    private static final String DEFAULT_TIMEZONE = "UTC";
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of(DEFAULT_TIMEZONE);

    private final BlobStorage blobStorage;
    private final ObjectMapper objectMapper;
    private final DlqConfig dlqConfig;

    public BlobStorageDlqWriter(BlobStorage blobStorage, DlqConfig dlqConfig) {
        this.blobStorage = blobStorage;
        this.objectMapper = new ObjectMapper();
        this.dlqConfig = dlqConfig;

        validateTimezoneConfiguration();
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
            try {
                blobStorage.store(objectName, data.getBytes(StandardCharsets.UTF_8));
            } catch (BlobStorageException e) {
                log.warn("Failed to store into DLQ messages into blob storage", e);
                failedMessages.addAll(partitionedMessages);
            }
        });
        return failedMessages;
    }

    private String convertToString(Message message) {
        try {
            return objectMapper.writeValueAsString(new DlqMessage(
                    Base64.getEncoder()
                            .encodeToString(message.getLogKey() == null ? "".getBytes() : message.getLogKey()),
                    Base64.getEncoder()
                            .encodeToString(message.getLogMessage() == null ? "".getBytes() : message.getLogMessage()),
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getTimestamp(),
                    message.getErrorInfo().toString(),
                    message.getErrorInfo().getErrorType().name()));
        } catch (JsonProcessingException e) {
            log.warn("Not able to convert message into json", e);
            return "";
        }
    }

    private Path createPartition(Message message) {
        ZoneId zoneId = getSafeZoneId();
        LocalDate consumeLocalDate = LocalDate.from(Instant.ofEpochMilli(message.getConsumeTimestamp())
                .atZone(zoneId));
        String consumeDate = DateTimeFormatter.ISO_LOCAL_DATE.format(consumeLocalDate);
        return Paths.get(message.getTopic(), consumeDate);
    }

    private ZoneId getSafeZoneId() {
        try {
            String configuredTimezone = dlqConfig.getDlqBlobFilePartitionTimezone();

            if (configuredTimezone == null || configuredTimezone.trim().isEmpty()) {
                log.warn("DLQ blob file partition timezone is null or empty, using default timezone: {}",
                        DEFAULT_TIMEZONE);
                return DEFAULT_ZONE_ID;
            }

            String trimmedTimezone = configuredTimezone.trim();
            ZoneId zoneId = ZoneId.of(trimmedTimezone);

            if (!zoneId.getId().equals(trimmedTimezone)) {
                log.warn(
                        "DLQ blob file partition timezone '{}' was normalized to '{}', consider using the normalized form",
                        trimmedTimezone, zoneId.getId());
            }

            return zoneId;

        } catch (ZoneRulesException e) {
            log.error("Invalid DLQ blob file partition timezone '{}', falling back to default timezone '{}'. Error: {}",
                    dlqConfig.getDlqBlobFilePartitionTimezone(), DEFAULT_TIMEZONE, e.getMessage());
            return DEFAULT_ZONE_ID;

        } catch (Exception e) {
            log.error(
                    "Unexpected error while getting DLQ blob file partition timezone, falling back to default timezone '{}'. Error: {}",
                    DEFAULT_TIMEZONE, e.getMessage());
            return DEFAULT_ZONE_ID;
        }
    }

    private void validateTimezoneConfiguration() {
        try {
            String configuredTimezone = dlqConfig.getDlqBlobFilePartitionTimezone();

            if (configuredTimezone == null || configuredTimezone.trim().isEmpty()) {
                log.warn(
                        "DLQ blob file partition timezone configuration is null or empty, will use default timezone '{}' for partitioning",
                        DEFAULT_TIMEZONE);
                return;
            }

            String trimmedTimezone = configuredTimezone.trim();
            ZoneId zoneId = ZoneId.of(trimmedTimezone);

            log.info("DLQ blob file partition timezone configuration validated successfully: '{}'", zoneId.getId());

            if (!zoneId.getId().equals(trimmedTimezone)) {
                log.warn("DLQ blob file partition timezone '{}' was normalized to '{}' during validation",
                        trimmedTimezone, zoneId.getId());
            }

        } catch (ZoneRulesException e) {
            log.error(
                    "Invalid DLQ blob file partition timezone configuration '{}', will fall back to '{}' during runtime. "
                            +
                            "Please check your configuration and provide a valid timezone identifier. Error: {}",
                    dlqConfig.getDlqBlobFilePartitionTimezone(), DEFAULT_TIMEZONE, e.getMessage());

        } catch (Exception e) {
            log.error("Unexpected error during DLQ blob file partition timezone configuration validation, "
                    + "will fall back to '{}' during runtime. Error: {}", DEFAULT_TIMEZONE, e.getMessage());
        }
    }
}
