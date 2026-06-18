package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utility for deriving the date partition string used by the blob storage dead letter queue.
 * <p>
 * Formats the chosen message timestamp (produce or consume) as an ISO local date ({@code yyyy-MM-dd})
 * in a given timezone.
 *
 * @see BlobStorageDlqWriter
 * @see DlqPartitionKeyType
 */
public class DlqDateUtils {

    /**
     * Returns the consume-timestamp date for a message in the given timezone.
     * <p>
     * Convenience overload that uses {@link DlqPartitionKeyType#CONSUME_TIMESTAMP}.
     *
     * @param message the message to derive the date from
     * @param zoneId the timezone to compute the date in
     * @return the ISO local date string
     */
    public static String getDateFromMessage(Message message, ZoneId zoneId) {
        return getDateFromMessage(
                message,
                zoneId,
                DlqPartitionKeyType.CONSUME_TIMESTAMP
        );
    }

    /**
     * Returns the partition date for a message in the given timezone.
     * <p>
     * Uses the produce timestamp when {@code partitionKeyType} is
     * {@link DlqPartitionKeyType#PRODUCE_TIMESTAMP} and that timestamp is positive; otherwise falls
     * back to the consume timestamp.
     *
     * @param message the message to derive the date from
     * @param zoneId the timezone to compute the date in
     * @param partitionKeyType which timestamp to partition by
     * @return the ISO local date string ({@code yyyy-MM-dd})
     */
    public static String getDateFromMessage(Message message, ZoneId zoneId, DlqPartitionKeyType partitionKeyType) {
        long timestamp =
                (partitionKeyType == DlqPartitionKeyType.PRODUCE_TIMESTAMP
                        && message.getTimestamp() > 0)
                        ? message.getTimestamp()
                        : message.getConsumeTimestamp();

        return Instant.ofEpochMilli(timestamp)
                .atZone(zoneId)
                .toLocalDate()
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
    }
}

