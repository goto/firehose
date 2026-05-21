package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.config.enums.TimePartitionType;
import com.gotocompany.firehose.sink.blob.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RecordTest {

    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final Instant messageTimeStamp = Instant.parse("2020-01-02T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";


    @Test
    public void shouldGetTopicFromMetaData() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals("booking-log", record.getTopic(""));
    }

    @Test
    public void shouldGetTopicFromNestedMetaData() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals("booking-log", record.getTopic("nested_field"));
    }

    @Test
    public void shouldGetTimeStampFromMessage() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals(defaultTimestamp, record.getTimestampFromMessage("created_time"));
    }

    @Test
    public void shouldGetDateTimeLocally() throws InterruptedException {
        BlobSinkConfig config = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(config.getFilePartitionTimeType()).thenReturn(TimePartitionType.PROCESSING_TIMESTAMP);
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        LocalDateTime before = LocalDateTime.now();
        Thread.sleep(1000);
        LocalDateTime localDateTime = record.getLocalDateTime(config);
        Thread.sleep(1000);
        LocalDateTime after = LocalDateTime.now();
        Assert.assertTrue(localDateTime.isAfter(before));
        Assert.assertTrue(localDateTime.isBefore(after));
    }

    @Test
    public void shouldGetDateTimeFromMessage() throws InterruptedException {
        BlobSinkConfig config = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(config.getFilePartitionTimeType()).thenReturn(TimePartitionType.EVENT_TIMESTAMP);
        Mockito.when(config.getFilePartitionProtoTimestampFieldName()).thenReturn("created_time");
        Mockito.when(config.getFilePartitionProtoTimestampTimezone()).thenReturn("UTC");
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        LocalDateTime localDateTime = record.getLocalDateTime(config);
        Assert.assertEquals(LocalDateTime.ofInstant(defaultTimestamp, ZoneId.of("UTC")), localDateTime);
    }
    @Test
    public void shouldGetDateTimeFromKafkaMessage() throws InterruptedException {
        BlobSinkConfig config = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(config.getFilePartitionTimeType()).thenReturn(TimePartitionType.MESSAGE_TIMESTAMP);
        Mockito.when(config.getOutputKafkaMetadataColumnName()).thenReturn("nested_field");
        Mockito.when(config.getFilePartitionProtoTimestampFieldName()).thenReturn("created_time");
        Mockito.when(config.getFilePartitionProtoTimestampTimezone()).thenReturn("UTC");
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", messageTimeStamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        LocalDateTime localDateTime = record.getLocalDateTime(config);
        Assert.assertEquals(LocalDateTime.ofInstant(messageTimeStamp, ZoneId.of("UTC")), localDateTime);
    }
}
