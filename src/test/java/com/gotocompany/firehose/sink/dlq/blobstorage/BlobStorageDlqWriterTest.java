package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
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
import java.util.Comparator;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageDlqWriterTest {

    @Mock
    private BlobStorage blobStorage;

    @Mock
    private DlqConfig dlqConfig;

    private BlobStorageDlqWriter blobStorageDLQWriter;

    @Before
    public void setUp() throws Exception {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));
        blobStorageDLQWriter = new BlobStorageDlqWriter(blobStorage, dlqConfig);
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

        BlobStorageDlqWriter writerWithTokyoTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

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

        BlobStorageDlqWriter writerWithNullTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

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

        BlobStorageDlqWriter writerWithEmptyTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

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

        BlobStorageDlqWriter writerWithWhitespaceTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

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

        BlobStorageDlqWriter writerWithWhitespaceAroundTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

        long utcTimestamp = Instant.parse("2020-01-01T15:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, utcTimestamp,
                utcTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message);
        writerWithWhitespaceAroundTimezone.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-02"), any(byte[].class));
    }

    @Test
    public void shouldHandleExtremeTimestamps() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("Asia/Tokyo"));

        BlobStorageDlqWriter writerWithTokyoTimezone = new BlobStorageDlqWriter(blobStorage, dlqConfig);

        long maxTimestamp = Long.MAX_VALUE;
        long minTimestamp = 0L;

        Message maxMessage = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, maxTimestamp,
                maxTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message minMessage = new Message("456".getBytes(), "def".getBytes(), "booking", 1, 2, null, minTimestamp,
                minTimestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        writerWithTokyoTimezone.write(Arrays.asList(maxMessage));
        writerWithTokyoTimezone.write(Arrays.asList(minMessage));

        verify(blobStorage, times(2)).store(anyString(), any(byte[].class));
    }

    @Test
    public void shouldHandleNullErrorInfoGracefully() throws IOException, BlobStorageException {
        when(dlqConfig.getDlqBlobFilePartitionTimezone()).thenReturn(ZoneId.of("UTC"));

        BlobStorageDlqWriter writer = new BlobStorageDlqWriter(blobStorage, dlqConfig);

        long utcTimestamp = Instant.parse("2020-01-01T12:00:00Z").toEpochMilli();
        Message messageWithNullErrorInfo = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null,
                utcTimestamp, utcTimestamp, null);

        List<Message> messages = Arrays.asList(messageWithNullErrorInfo);
        writer.write(messages);

        verify(blobStorage).store(contains("booking/2020-01-01"), any(byte[].class));
    }
}
