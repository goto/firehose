package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DlqDateUtils {

    public static String getDateFromMessage(Message message, ZoneId zoneId) {
        return getDateFromMessage(
                message,
                zoneId,
                DlqPartitionKeyType.CONSUME_TIMESTAMP
        );
    }

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

