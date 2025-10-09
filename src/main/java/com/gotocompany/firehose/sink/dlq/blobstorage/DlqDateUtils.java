package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.firehose.message.Message;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DlqDateUtils {

    public static String getDateFromMessage(Message message, ZoneId zoneId) {
        LocalDate consumeLocalDate = LocalDate.from(Instant.ofEpochMilli(message.getConsumeTimestamp()).atZone(zoneId));
        return DateTimeFormatter.ISO_LOCAL_DATE.format(consumeLocalDate);
    }
}

