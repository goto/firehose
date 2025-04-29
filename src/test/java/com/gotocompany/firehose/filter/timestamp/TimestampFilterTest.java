package com.gotocompany.firehose.filter.timestamp;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TimestampFilterTest {

    @Mock
    private StencilClient stencilClient;

    @Mock
    private FilterConfig filterConfig;

    @Mock
    private FirehoseInstrumentation instrumentation;

    @Mock
    private Parser parser;

    @Mock
    private DynamicMessage dynamicMessage;

    @Mock
    private Descriptors.Descriptor descriptor;

    @Mock
    private Descriptors.FieldDescriptor fieldDescriptor;

    private TimestampFilter timestampFilter;
    private static final String PROTO_CLASS = "com.test.TestMessage";
    private static final String TIMESTAMP_FIELD = "event_timestamp";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(filterConfig.getFilterDataSource()).thenReturn(FilterDataSourceType.MESSAGE);
        when(filterConfig.getFilterTimestampFieldName()).thenReturn(TIMESTAMP_FIELD);
        when(filterConfig.getFilterDropDeserializationError()).thenReturn(true);
        when(filterConfig.getFilterTimestampPastWindow()).thenReturn(604800L);
        when(filterConfig.getFilterTimestampFutureWindow()).thenReturn(604800L);
        when(filterConfig.getFilterSchemaProtoClass()).thenReturn(PROTO_CLASS);

        when(stencilClient.getParser(PROTO_CLASS)).thenReturn(parser);

        when(dynamicMessage.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName(TIMESTAMP_FIELD)).thenReturn(fieldDescriptor);

        when(instrumentation.isDebugEnabled()).thenReturn(true);
    }

    @Test
    public void testConstructorSuccess() {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
        assertNotNull(timestampFilter);

        verify(instrumentation, times(7)).logInfo(anyString(), any());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullProtoClass() {
        when(filterConfig.getFilterSchemaProtoClass()).thenReturn(null);
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyProtoClass() {
        when(filterConfig.getFilterSchemaProtoClass()).thenReturn("");
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorParserCreationFailed() {
        when(stencilClient.getParser(PROTO_CLASS)).thenReturn(null);
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
    }

    @Test
    public void testFilterNullMessages() throws FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
        FilteredMessages result = timestampFilter.filter(null);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());

        verify(instrumentation).logWarn("Received null message list to filter");
    }

    @Test
    public void testFilterEmptyMessages() throws FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);
        FilteredMessages result = timestampFilter.filter(Collections.emptyList());

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());

        verify(instrumentation).logDebug("Received empty message list to filter");
    }

    @Test
    public void testFilterNullMessage() throws FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        Message nullMessage = null;
        List<Message> messages = Collections.singletonList(nullMessage);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).logWarn("Encountered null message. Skipping.");
        verify(instrumentation).captureCount(contains("invalid_messages_total"), eq(1L));
    }

    @Test
    public void testFilterEmptyMessageData() throws FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        Message emptyDataMessage = new Message(new byte[0], new byte[0], "topic", 0, 0);
        List<Message> messages = Collections.singletonList(emptyDataMessage);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).logWarn(contains("Message has empty data"), eq("MESSAGE"));
    }

    @Test
    public void testFilterDeserializationErrorDrop() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "test".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenThrow(new InvalidProtocolBufferException("Test error"));

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("deserialization_errors_total"), eq(1L));
        verify(instrumentation).logWarn(contains("Failed to deserialize message"), anyString());
    }

    @Test(expected = FilterException.class)
    public void testFilterDeserializationErrorThrowException() throws InvalidProtocolBufferException, FilterException {
        when(filterConfig.getFilterDropDeserializationError()).thenReturn(false);
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "test".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenThrow(new InvalidProtocolBufferException("Test error"));

        timestampFilter.filter(messages);
    }

    @Test
    public void testFilterValidTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "valid".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("valid_messages_total"), eq(1L));
    }

    @Test
    public void testFilterTimestampTooOld() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "old".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long oldTimestamp = currentTime - 604800L - 100;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(oldTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("invalid_timestamp_errors_total"), eq(1L));
        verify(instrumentation).logDebug(contains("Message filtered out"), anyLong(), anyLong());
    }

    @Test
    public void testFilterTimestampTooFuture() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "future".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long futureTimestamp = currentTime + 604800L + 100;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(futureTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("invalid_timestamp_errors_total"), eq(1L));
        verify(instrumentation).logDebug(contains("too far in future"), anyLong(), anyLong());
    }

    @Test
    public void testFilterTimestampFieldNotPresent() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "missing".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(false);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("invalid_timestamp_errors_total"), eq(1L));
        verify(instrumentation).logDebug(contains("does not contain the timestamp field"), eq(TIMESTAMP_FIELD));
    }

    @Test
    public void testFilterTimestampFieldNull() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "null".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(null);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("invalid_timestamp_errors_total"), eq(1L));
        verify(instrumentation).logDebug(contains("has null value"), eq(TIMESTAMP_FIELD));
    }

    @Test
    public void testFilterFieldNotFoundInDescriptor() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "no_field".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(descriptor.findFieldByName(TIMESTAMP_FIELD)).thenReturn(null);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("unknown_field_errors_total"), eq(1L));
        verify(instrumentation).logWarn(eq("Field '{}' not found in message type '{}'"), eq(TIMESTAMP_FIELD), any());
    }

    @Test
    public void testFilterIntegerTimestampValue() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "integer".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        Integer intTimestamp = (int) currentTime;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(intTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterDateTimestampValue() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "date".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        Date dateTimestamp = new Date();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(dateTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterStringNumericTimestampValue() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "string_numeric".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        String stringTimestamp = String.valueOf(Instant.now().getEpochSecond());
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(stringTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterStringIsoTimestampValue() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "string_iso".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        String isoTimestamp = Instant.now().toString();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(isoTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterInvalidStringTimestampFormat() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "invalid_string".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        String invalidTimestamp = "not-a-timestamp";
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(invalidTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterUnsupportedTimestampType() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "unsupported".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(new Object());

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("unsupported_type_errors_total"), eq(1L));
    }

    @Test
    public void testFilterKeyDataSource() throws InvalidProtocolBufferException, FilterException {
        when(filterConfig.getFilterDataSource()).thenReturn(FilterDataSourceType.KEY);
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] keyData = "key".getBytes();
        byte[] messageData = "message".getBytes();
        Message message = new Message(keyData, messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(keyData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterEdgesOfTimestampWindow() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        List<Message> messages = new ArrayList<>();

        long currentTime = Instant.now().getEpochSecond();
        long pastEdge = currentTime - 604800L;
        long futureEdge = currentTime + 604800L;

        byte[] pastData = "past_edge".getBytes();
        Message pastMessage = new Message(new byte[1], pastData, "topic", 0, 0);
        messages.add(pastMessage);

        byte[] futureData = "future_edge".getBytes();
        Message futureMessage = new Message(new byte[1], futureData, "topic", 0, 0);
        messages.add(futureMessage);

        DynamicMessage pastDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage futureDynamicMsg = mock(DynamicMessage.class);

        when(parser.parse(pastData)).thenReturn(pastDynamicMsg);
        when(parser.parse(futureData)).thenReturn(futureDynamicMsg);

        when(pastDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(futureDynamicMsg.getDescriptorForType()).thenReturn(descriptor);

        when(pastDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(futureDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);

        when(pastDynamicMsg.getField(fieldDescriptor)).thenReturn(pastEdge);
        when(futureDynamicMsg.getField(fieldDescriptor)).thenReturn(futureEdge);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(2, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterCustomWindowSizes() throws InvalidProtocolBufferException, FilterException {
        when(filterConfig.getFilterTimestampPastWindow()).thenReturn(3600L);
        when(filterConfig.getFilterTimestampFutureWindow()).thenReturn(1800L);

        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        List<Message> messages = new ArrayList<>();
        long currentTime = Instant.now().getEpochSecond();

        byte[] validData = "valid".getBytes();
        byte[] pastData = "past".getBytes();
        byte[] futureData = "future".getBytes();

        Message validMessage = new Message(new byte[1], validData, "topic", 0, 0);
        Message pastMessage = new Message(new byte[1], pastData, "topic", 0, 0);
        Message futureMessage = new Message(new byte[1], futureData, "topic", 0, 0);

        messages.add(validMessage);
        messages.add(pastMessage);
        messages.add(futureMessage);

        DynamicMessage validDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage pastDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage futureDynamicMsg = mock(DynamicMessage.class);

        when(parser.parse(validData)).thenReturn(validDynamicMsg);
        when(parser.parse(pastData)).thenReturn(pastDynamicMsg);
        when(parser.parse(futureData)).thenReturn(futureDynamicMsg);

        when(validDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(pastDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(futureDynamicMsg.getDescriptorForType()).thenReturn(descriptor);

        when(validDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(pastDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(futureDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);

        when(validDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime - 1800L);
        when(pastDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime - 7200L);
        when(futureDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime + 2700L);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(2, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterMultipleMessagesMixedValidity() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        List<Message> messages = new ArrayList<>();
        long currentTime = Instant.now().getEpochSecond();

        byte[] validData = "valid".getBytes();
        Message validMessage = new Message(new byte[1], validData, "topic", 0, 0);
        messages.add(validMessage);

        byte[] oldData = "old".getBytes();
        Message oldMessage = new Message(new byte[1], oldData, "topic", 0, 0);
        messages.add(oldMessage);

        byte[] errorData = "error".getBytes();
        Message errorMessage = new Message(new byte[1], errorData, "topic", 0, 0);
        messages.add(errorMessage);

        DynamicMessage validDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage oldDynamicMsg = mock(DynamicMessage.class);

        when(parser.parse(validData)).thenReturn(validDynamicMsg);
        when(parser.parse(oldData)).thenReturn(oldDynamicMsg);
        when(parser.parse(errorData)).thenThrow(new InvalidProtocolBufferException("Test error"));

        when(validDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(oldDynamicMsg.getDescriptorForType()).thenReturn(descriptor);

        when(validDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(oldDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);

        when(validDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime);
        when(oldDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime - 604800L - 100);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(2, result.getInvalidMessages().size());

        verify(instrumentation).captureCount(contains("messages_processed_total"), eq(3L));
        verify(instrumentation).captureCount(contains("valid_messages_total"), eq(1L));
        verify(instrumentation).captureCount(contains("invalid_messages_total"), eq(2L));
    }

    @Test
    public void testFilterWithMillisecondTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "milliseconds".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithExactBoundaryPastTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "exact_past_boundary".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long exactPastBoundary = currentTime - 604800L;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(exactPastBoundary);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithExactBoundaryFutureTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "exact_future_boundary".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long exactFutureBoundary = currentTime + 604800L;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(exactFutureBoundary);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithJustBeyondPastBoundaryTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "beyond_past_boundary".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long justBeyondPastBoundary = currentTime - 604800L - 1;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(justBeyondPastBoundary);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithJustBeyondFutureBoundaryTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "beyond_future_boundary".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long justBeyondFutureBoundary = currentTime + 604800L + 1;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(justBeyondFutureBoundary);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithZeroTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "zero_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(0L);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithNegativeTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "negative_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(-1L);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithIso8601FormatWithTimezone() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "iso8601_with_timezone".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithExtremelyLargeTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "large_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long extremelyLargeTimestamp = Long.MAX_VALUE;
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(extremelyLargeTimestamp);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithMalformedIsoTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "malformed_iso".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn("2023-13-45T25:70:80Z");

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithEmptyStringTimestamp() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "empty_string_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn("");

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithMockProtobufTimestamp() throws Exception {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "protobuf_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        long currentTime = Instant.now().getEpochSecond();

        when(parser.parse(messageData)).thenReturn(dynamicMessage);
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithVeryShortWindowSizes() throws InvalidProtocolBufferException, FilterException {
        when(filterConfig.getFilterTimestampPastWindow()).thenReturn(1L);
        when(filterConfig.getFilterTimestampFutureWindow()).thenReturn(1L);

        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] currentData = "current".getBytes();
        Message currentMessage = new Message(new byte[1], currentData, "topic", 0, 0);

        byte[] oldData = "old".getBytes();
        Message oldMessage = new Message(new byte[1], oldData, "topic", 0, 0);

        byte[] futureData = "future".getBytes();
        Message futureMessage = new Message(new byte[1], futureData, "topic", 0, 0);

        List<Message> messages = Arrays.asList(currentMessage, oldMessage, futureMessage);

        long currentTime = Instant.now().getEpochSecond();

        DynamicMessage currentDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage oldDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage futureDynamicMsg = mock(DynamicMessage.class);

        when(parser.parse(currentData)).thenReturn(currentDynamicMsg);
        when(parser.parse(oldData)).thenReturn(oldDynamicMsg);
        when(parser.parse(futureData)).thenReturn(futureDynamicMsg);

        when(currentDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(oldDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(futureDynamicMsg.getDescriptorForType()).thenReturn(descriptor);

        when(currentDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(oldDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(futureDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);

        when(currentDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime);
        when(oldDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime - 2);
        when(futureDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime + 2);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(2, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithVeryLargeWindowSizes() throws InvalidProtocolBufferException, FilterException {
        long tenYearsInSeconds = 10L * 365 * 24 * 60 * 60;
        when(filterConfig.getFilterTimestampPastWindow()).thenReturn(tenYearsInSeconds);
        when(filterConfig.getFilterTimestampFutureWindow()).thenReturn(tenYearsInSeconds);

        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "very_old_but_valid".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        long currentTime = Instant.now().getEpochSecond();
        long fiveYearsAgo = currentTime - (5L * 365 * 24 * 60 * 60);
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(fiveYearsAgo);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithZeroWindowSizes() throws InvalidProtocolBufferException, FilterException {
        when(filterConfig.getFilterTimestampPastWindow()).thenReturn(0L);
        when(filterConfig.getFilterTimestampFutureWindow()).thenReturn(0L);

        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] exactData = "exact_current".getBytes();
        Message exactMessage = new Message(new byte[1], exactData, "topic", 0, 0);

        byte[] slightlyOffData = "slightly_off".getBytes();
        Message slightlyOffMessage = new Message(new byte[1], slightlyOffData, "topic", 0, 0);

        List<Message> messages = Arrays.asList(exactMessage, slightlyOffMessage);

        long currentTime = Instant.now().getEpochSecond();

        DynamicMessage exactDynamicMsg = mock(DynamicMessage.class);
        DynamicMessage slightlyOffDynamicMsg = mock(DynamicMessage.class);

        when(parser.parse(exactData)).thenReturn(exactDynamicMsg);
        when(parser.parse(slightlyOffData)).thenReturn(slightlyOffDynamicMsg);

        when(exactDynamicMsg.getDescriptorForType()).thenReturn(descriptor);
        when(slightlyOffDynamicMsg.getDescriptorForType()).thenReturn(descriptor);

        when(exactDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);
        when(slightlyOffDynamicMsg.hasField(fieldDescriptor)).thenReturn(true);

        when(exactDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime);
        when(slightlyOffDynamicMsg.getField(fieldDescriptor)).thenReturn(currentTime - 1);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertTrue(result.getValidMessages().size() <= 1);
        assertTrue(result.getInvalidMessages().size() >= 1);
    }

    @Test
    public void testWithLargeBatchOfMessages() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        List<Message> messages = new ArrayList<>();
        List<DynamicMessage> dynamicMessages = new ArrayList<>();
        List<byte[]> messageDatas = new ArrayList<>();

        long currentTime = Instant.now().getEpochSecond();

        for (int i = 0; i < 100; i++) {
            byte[] data = ("message" + i).getBytes();
            messageDatas.add(data);

            Message message = new Message(new byte[1], data, "topic", 0, i);
            messages.add(message);

            DynamicMessage dynMsg = mock(DynamicMessage.class);
            dynamicMessages.add(dynMsg);

            when(dynMsg.getDescriptorForType()).thenReturn(descriptor);
            when(dynMsg.hasField(fieldDescriptor)).thenReturn(true);

            if (i % 3 == 0) {
                when(dynMsg.getField(fieldDescriptor)).thenReturn(currentTime - 604800L - 10);
            } else {
                when(dynMsg.getField(fieldDescriptor)).thenReturn(currentTime);
            }

            when(parser.parse(data)).thenReturn(dynMsg);
        }

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(66, result.getValidMessages().size());
        assertEquals(34, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithProtobufTimestampAsDynamicMessage() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "dynamic_protobuf_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        DynamicMessage timestampDynamicMsg = mock(DynamicMessage.class);
        Descriptors.Descriptor timestampDescriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor secondsFieldDescriptor = mock(Descriptors.FieldDescriptor.class);
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(timestampDynamicMsg);
        when(timestampDynamicMsg.getDescriptorForType()).thenReturn(timestampDescriptor);
        when(timestampDescriptor.getFullName()).thenReturn("google.protobuf.Timestamp");
        when(timestampDescriptor.findFieldByName("seconds")).thenReturn(secondsFieldDescriptor);
        long currentTime = Instant.now().getEpochSecond();
        when(timestampDynamicMsg.hasField(secondsFieldDescriptor)).thenReturn(true);
        when(timestampDynamicMsg.getField(secondsFieldDescriptor)).thenReturn(currentTime);

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(1, result.getValidMessages().size());
        assertEquals(0, result.getInvalidMessages().size());
        verify(timestampDynamicMsg).getField(secondsFieldDescriptor);
    }

    @Test
    public void testFilterWithInvalidProtobufTimestampAsDynamicMessage() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "invalid_dynamic_protobuf_timestamp".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        DynamicMessage timestampDynamicMsg = mock(DynamicMessage.class);
        Descriptors.Descriptor timestampDescriptor = mock(Descriptors.Descriptor.class);
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(timestampDynamicMsg);
        when(timestampDynamicMsg.getDescriptorForType()).thenReturn(timestampDescriptor);
        when(timestampDescriptor.getFullName()).thenReturn("google.protobuf.Timestamp");
        when(timestampDescriptor.findFieldByName("seconds")).thenReturn(null); // seconds field not found

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
    }

    @Test
    public void testFilterWithNonTimestampDynamicMessage() throws InvalidProtocolBufferException, FilterException {
        timestampFilter = new TimestampFilter(stencilClient, filterConfig, instrumentation);

        byte[] messageData = "non_timestamp_dynamic_message".getBytes();
        Message message = new Message(new byte[1], messageData, "topic", 0, 0);
        List<Message> messages = Collections.singletonList(message);

        when(parser.parse(messageData)).thenReturn(dynamicMessage);

        DynamicMessage nonTimestampDynamicMsg = mock(DynamicMessage.class);
        Descriptors.Descriptor otherDescriptor = mock(Descriptors.Descriptor.class);
        when(dynamicMessage.hasField(fieldDescriptor)).thenReturn(true);
        when(dynamicMessage.getField(fieldDescriptor)).thenReturn(nonTimestampDynamicMsg);
        when(nonTimestampDynamicMsg.getDescriptorForType()).thenReturn(otherDescriptor);
        when(otherDescriptor.getFullName()).thenReturn("com.example.SomeOtherMessage");

        FilteredMessages result = timestampFilter.filter(messages);

        assertNotNull(result);
        assertEquals(0, result.getValidMessages().size());
        assertEquals(1, result.getInvalidMessages().size());
        verify(instrumentation).captureCount(contains("unsupported_type_errors_total"), eq(1L));
    }
}
