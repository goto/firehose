package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.message.Message;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class DlqDateUtilsTest {

    @Test
    public void shouldReturnDateInUTC() {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String date = DlqDateUtils.getDateFromMessage(message, ZoneId.of("UTC"));

        assertEquals("2020-01-01", date);
    }

    @Test
    public void shouldReturnDateInTokyoTimezone() {
        long utcTimestamp = Instant.parse("2020-01-01T15:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp, utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String date = DlqDateUtils.getDateFromMessage(message, ZoneId.of("Asia/Tokyo"));

        assertEquals("2020-01-02", date);
    }

    @Test
    public void shouldReturnDateInNewYorkTimezone() {
        long utcTimestamp = Instant.parse("2020-01-02T00:30:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp, utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String date = DlqDateUtils.getDateFromMessage(message, ZoneId.of("America/New_York"));

        assertEquals("2020-01-01", date);
    }

    @Test
    public void shouldHandleEpochZero() {
        long timestamp = 0L;
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String date = DlqDateUtils.getDateFromMessage(message, ZoneId.of("UTC"));

        assertEquals("1970-01-01", date);
    }

    @Test
    public void shouldReturnDifferentDatesForDifferentTimestamps() {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1, timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp2, timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String date1 = DlqDateUtils.getDateFromMessage(message1, ZoneId.of("UTC"));
        String date2 = DlqDateUtils.getDateFromMessage(message2, ZoneId.of("UTC"));

        assertEquals("2020-01-01", date1);
        assertEquals("2020-01-02", date2);
    }
}

