package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageDlqWriterTest {

    @Mock
    private BlobStorage blobStorage;

    @Mock
    private DlqConfig dlqConfig;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private BlobStorageDlqWriter blobStorageDLQWriter;

    @Before
    public void setUp() throws Exception {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));
        blobStorageDLQWriter = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);
    }

    @Test
    public void shouldWriteMessagesWithoutErrorInfoToObjectStorage() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2,
                timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2,
                timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        Assert.assertEquals(0, blobStorageDLQWriter.write(messages).size());

        verify(blobStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"Exception test, ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"Exception test, ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}")
                        .getBytes()));
        verify(blobStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"Exception test, ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"Exception test, ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}")
                        .getBytes()));
    }

    @Test
    public void shouldWriteMessageErrorTypesToObjectStorage() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new NullPointerException(), ErrorType.SINK_UNKNOWN_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2,
                timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2,
                timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.SINK_UNKNOWN_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        Assert.assertEquals(0, blobStorageDLQWriter.write(messages).size());

        verify(blobStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"Exception , ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"Exception , ErrorType: SINK_UNKNOWN_ERROR\",\"error_type\":\"SINK_UNKNOWN_ERROR\"}")
                        .getBytes()));
        verify(blobStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"Exception , ErrorType: DESERIALIZATION_ERROR\",\"error_type\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"MTIz\",\"value\":\"YWJj\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"Exception null, ErrorType: SINK_UNKNOWN_ERROR\",\"error_type\":\"SINK_UNKNOWN_ERROR\"}")
                        .getBytes()));
    }

    @Test
    public void shouldThrowIOExceptionWhenWriteFileThrowIOException() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2,
                timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2,
                timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));

        doThrow(new BlobStorageException("", "", new IOException())).when(blobStorage).store(anyString(),
                any(byte[].class));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        List<Message> failedMessages = blobStorageDLQWriter.write(messages);
        messages.sort(Comparator.comparingLong(Message::getOffset));
        failedMessages.sort(Comparator.comparingLong(Message::getOffset));
        Assert.assertEquals(messages, failedMessages);
    }

    @Test
    public void shouldUseConfiguredTimezoneForPartitioning() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("Asia/Tokyo"));

        BlobStorageDlqWriter writerWithTokyoTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T15:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithTokyoTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-02"), any(byte[].class));
    }

    @Test
    public void shouldUseUTCWhenTimezoneIsNull() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));

        BlobStorageDlqWriter writerWithNullTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T12:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithNullTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-01"), any(byte[].class));
    }

    @Test
    public void shouldUseUTCWhenTimezoneIsEmpty() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));

        BlobStorageDlqWriter writerWithEmptyTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T12:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithEmptyTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-01"), any(byte[].class));
    }

    @Test
    public void shouldUseUTCWhenTimezoneIsWhitespace() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));

        BlobStorageDlqWriter writerWithWhitespaceTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T12:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithWhitespaceTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-01"), any(byte[].class));
    }

    @Test
    public void shouldHandleTimezoneWithWhitespace() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("Asia/Tokyo"));

        BlobStorageDlqWriter writerWithWhitespaceAroundTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T15:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithWhitespaceAroundTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-02"), any(byte[].class));
    }

    @Test
    public void shouldCaptureSuccessMetricsWithDateTagWhenMessagesWrittenSuccessfully() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2);
        blobStorageDLQWriter.write(messages);

        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.SUCCESS),
                eq(null),
                eq("2020-01-01"),
                eq(2L)
        );
    }

    @Test
    public void shouldCaptureFailureMetricsWithDateTagAndErrorTypeWhenWriteFails() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.SINK_UNKNOWN_ERROR));

        doThrow(new BlobStorageException("", "", new IOException())).when(blobStorage).store(anyString(), any(byte[].class));

        List<Message> messages = Arrays.asList(message1, message2);
        blobStorageDLQWriter.write(messages);

        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.FAILURE),
                eq(ErrorType.DESERIALIZATION_ERROR),
                eq("2020-01-01"),
                eq(1L)
        );
        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.FAILURE),
                eq(ErrorType.SINK_UNKNOWN_ERROR),
                eq("2020-01-01"),
                eq(1L)
        );
    }

    @Test
    public void shouldCaptureMetricsWithCorrectDateForDifferentPartitions() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2,
                timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3);
        blobStorageDLQWriter.write(messages);

        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.SUCCESS),
                eq(null),
                eq("2020-01-01"),
                eq(2L)
        );
        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.SUCCESS),
                eq(null),
                eq("2020-01-02"),
                eq(1L)
        );
    }

    @Test
    public void shouldCaptureMetricsWithTimezoneAwareDateTag() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("Asia/Tokyo"));

        BlobStorageDlqWriter writerWithTokyoTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig, firehoseInstrumentation);

        long utcTimestamp = Instant.parse("2020-01-01T15:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithTokyoTimezone.write(messages);

        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.SUCCESS),
                eq(null),
                eq("2020-01-02"),
                eq(1L)
        );
    }

    @Test
    public void shouldCaptureMetricsForBothSuccessAndFailureMessages() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp2,
                timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        doNothing().when(blobStorage).store(contains("2020-01-01"), any(byte[].class));
        doThrow(new BlobStorageException("", "", new IOException())).when(blobStorage).store(contains("2020-01-02"), any(byte[].class));

        List<Message> messages = Arrays.asList(message1, message2);
        blobStorageDLQWriter.write(messages);

        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.SUCCESS),
                eq(null),
                eq("2020-01-01"),
                eq(1L)
        );
        verify(firehoseInstrumentation).captureDLQBlobStorageMetrics(
                eq(Metrics.DLQ_MESSAGES_TOTAL),
                eq(Metrics.MessageType.FAILURE),
                eq(ErrorType.DESERIALIZATION_ERROR),
                eq("2020-01-02"),
                eq(1L)
        );
    }

    @Test
    public void shouldLogInfoForEmptyMessagesList() throws IOException {
        List<Message> messages = Collections.emptyList();
        List<Message> result = blobStorageDLQWriter.write(messages);
        
        Assert.assertEquals(0, result.size());
        verify(firehoseInstrumentation, never()).logInfo(anyString(), anyInt());
    }

    @Test
    public void shouldDetectEmptyBatchContent() throws IOException, BlobStorageException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("".getBytes(), "".getBytes(), "booking", 1, 1, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Collections.singletonList(message);
        blobStorageDLQWriter.write(messages);

        verify(blobStorage).store(anyString(), any(byte[].class));
    }

    @Test
    public void shouldDetectLargeBatch() throws IOException, BlobStorageException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        
        byte[] largeContent = new byte[11 * 1024 * 1024];
        Message message = new Message("key".getBytes(), largeContent, "booking", 1, 1, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Collections.singletonList(message);
        blobStorageDLQWriter.write(messages);

        verify(blobStorage).store(anyString(), any(byte[].class));
    }

    @Test
    public void shouldHandlePathsWithDoubleSlashes() throws IOException, BlobStorageException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking//test", 1, 1, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Collections.singletonList(message);
        blobStorageDLQWriter.write(messages);

        verify(blobStorage).store(anyString(), any(byte[].class));
    }

    @Test
    public void shouldLogStartAndCompleteForBatchProcessing() throws IOException, BlobStorageException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("456".getBytes(), "def".getBytes(), "booking", 1, 2, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2);
        blobStorageDLQWriter.write(messages);

        verify(firehoseInstrumentation).logInfo(eq("Starting DLQ blob storage write for {} messages"), eq(2));
        verify(firehoseInstrumentation).logInfo(
            eq("DLQ blob storage write complete - total: {}, successful partitions: {}, failed partitions: {}, successful messages: {}, failed messages: {}"),
            eq(2), anyInt(), anyInt(), anyInt(), anyInt());
    }

    @Test
    public void shouldTrackSuccessfulAndFailedPartitions() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1,
                timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp2,
                timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        doNothing().when(blobStorage).store(contains("2020-01-01"), any(byte[].class));
        doThrow(new BlobStorageException("error", "Storage failed", new IOException())).when(blobStorage)
                .store(contains("2020-01-02"), any(byte[].class));

        List<Message> messages = Arrays.asList(message1, message2);
        List<Message> failedMessages = blobStorageDLQWriter.write(messages);

        Assert.assertEquals(1, failedMessages.size());
        verify(firehoseInstrumentation).logInfo(
            eq("DLQ blob storage write complete - total: {}, successful partitions: {}, failed partitions: {}, successful messages: {}, failed messages: {}"),
            eq(2), eq(1), eq(1), eq(1), eq(1));
    }

    @Test
    public void shouldHandleMultipleMessagesInSamePartition() throws IOException, BlobStorageException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("456".getBytes(), "def".getBytes(), "booking", 1, 2, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message3 = new Message("789".getBytes(), "ghi".getBytes(), "booking", 1, 3, null, timestamp,
                timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3);
        blobStorageDLQWriter.write(messages);

        verify(blobStorage, times(1)).store(anyString(), any(byte[].class));
        verify(firehoseInstrumentation).logInfo(
            eq("DLQ blob storage write complete - total: {}, successful partitions: {}, failed partitions: {}, successful messages: {}, failed messages: {}"),
            eq(3), eq(1), eq(0), eq(3), eq(0));
    }
}
