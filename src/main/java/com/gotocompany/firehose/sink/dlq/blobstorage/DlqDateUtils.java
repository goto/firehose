package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DlqDateUtils {

    public static String getDateFromMessage(Message message, ZoneId zoneId) {
        LocalDate consumeLocalDate = LocalDate.from(Instant.ofEpochMilli(message.getConsumeTimestamp()).atZone(zoneId));
        return DateTimeFormatter.ISO_LOCAL_DATE.format(consumeLocalDate);
    }

    public static String getDateFromMessage(Message message, ZoneId zoneId, DlqPartitionKeyType partitionKeyType) {
        long timestamp;
        if (partitionKeyType == DlqPartitionKeyType.PRODUCE_TIMESTAMP) {
            timestamp = message.getTimestamp();
            if (timestamp <= 0) {
                timestamp = message.getConsumeTimestamp();
            }
        } else {
            timestamp = message.getConsumeTimestamp();
        }
        LocalDate localDate = LocalDate.from(Instant.ofEpochMilli(timestamp).atZone(zoneId));
        return DateTimeFormatter.ISO_LOCAL_DATE.format(localDate);
    }
}

