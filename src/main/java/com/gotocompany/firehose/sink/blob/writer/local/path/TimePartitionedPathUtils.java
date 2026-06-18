package com.gotocompany.firehose.sink.blob.writer.local.path;

import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.Constants;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Create path partition from Record.
 */
@AllArgsConstructor
public class TimePartitionedPathUtils {

    /** Formatter for the date segment, {@code yyyy-MM-dd}. */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /** Formatter for the hour segment, {@code HH}. */
    public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");

    /**
     * Builds the output path for a record according to the configured partitioning.
     * <p>
     * Reads the topic from the record's metadata and the partitioning timestamp from the configured
     * payload field. For {@code NONE} granularity the path is just the topic; for {@code DAY} it
     * appends a date segment; for {@code HOUR} it appends date and hour segments. Date and hour are
     * computed in the configured timezone.
     *
     * @param record the record to derive the path from
     * @param sinkConfig the blob sink configuration controlling granularity, prefixes and timezone
     * @return the relative time-partitioned path for the record
     * @throws IllegalArgumentException if the configured granularity is not recognised
     */
    public static Path getTimePartitionedPath(Record record, BlobSinkConfig sinkConfig) {
        String topic = record.getTopic(sinkConfig.getOutputKafkaMetadataColumnName());
        Instant timestamp = record.getTimestamp(sinkConfig.getFilePartitionProtoTimestampFieldName());
        if (sinkConfig.getFilePartitionTimeGranularityType() == Constants.FilePartitionType.NONE) {
            return Paths.get(topic);
        }
        LocalDate localDate = LocalDateTime.ofInstant(timestamp, ZoneId.of(sinkConfig.getFilePartitionProtoTimestampTimezone())).toLocalDate();
        String datePart = DATE_FORMATTER.format(localDate);
        LocalTime localTime = LocalDateTime.ofInstant(timestamp, ZoneId.of(sinkConfig.getFilePartitionProtoTimestampTimezone())).toLocalTime();
        String hourPart = HOUR_FORMATTER.format(localTime);

        String dateSegment = String.format("%s%s", sinkConfig.getFilePartitionTimeDatePrefix(), datePart);
        String hourSegment = String.format("%s%s", sinkConfig.getFilePartitionTimeHourPrefix(), hourPart);

        String dateTimePartition;
        switch (sinkConfig.getFilePartitionTimeGranularityType()) {
            case NONE:
                return Paths.get(topic);
            case DAY:
                dateTimePartition = String.format("%s", dateSegment);
                break;
            case HOUR:
                dateTimePartition = String.format("%s/%s", dateSegment, hourSegment);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return Paths.get(String.format("%s/%s", topic, dateTimePartition));
    }

}
