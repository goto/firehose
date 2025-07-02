package com.gotocompany.firehose.filter.json;

import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.consumer.TestBookingLogKey;
import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestKey;
import com.gotocompany.firehose.consumer.TestLocation;
import com.gotocompany.firehose.consumer.TestMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class JsonFilterTest {

    private FilterConfig filterConfig;
    private JsonFilter jsonFilter;
    private TestMessage testMessageProto1, testMessageProto2;
    private TestKey testKeyProto1, testKeyProto2;
    private String testMessageJson1, testMessageJson2;
    private String testKeyJson1, testKeyJson2;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
    private StencilClient stencilClient;

    @Before
    public void setup() throws InvalidProtocolBufferException {
        stencilClient = StencilClientFactory.getClient();
        firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        testKeyProto1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        testMessageProto1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        testKeyJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\"}";
        testMessageJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\",\"order_details\":\"details\"}";
        testKeyProto2 = TestKey.newBuilder().setOrderNumber("92").setOrderUrl("pqr").build();
        testMessageProto2 = TestMessage.newBuilder().setOrderNumber("92").setOrderUrl("pqr").setOrderDetails("details").build();
        testKeyJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\"}";
        testMessageJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\",\"order_details\":\"details\"}";
    }

    @Test
    public void shouldFilterEsbMessagesForProtobufMessageType() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToInvalidMessages(message2);
        assertEquals(1, filteredMessages.sizeOfValidMessages());
        assertEquals(1, filteredMessages.sizeOfInvalidMessages());
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldFilterMessagesWithNestedFieldsForProtobufMessageType() throws FilterException {
        TestBookingLogMessage testBookingLogMessage1 = TestBookingLogMessage
                .newBuilder()
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(22.4).build())
                .build();
        TestBookingLogMessage testBookingLogMessage2 = TestBookingLogMessage
                .newBuilder()
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(2222222.4).build())
                .build();

        Message message1 = new Message(testKeyProto1.toByteArray(), testBookingLogMessage1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testBookingLogMessage2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"driver_pickup_location\":{\"properties\":{\"latitude\":{\"minimum\":88}}}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestBookingLogMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message2);
        expectedMessages.addToInvalidMessages(message1);
        assertEquals(1, filteredMessages.sizeOfValidMessages());
        assertEquals(1, filteredMessages.sizeOfInvalidMessages());
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldFilterEsbMessagesForJsonMessageType() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToInvalidMessages(message2);
        FilteredMessages filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        assertEquals(1, filteredMessages.sizeOfValidMessages());
        assertEquals(1, filteredMessages.sizeOfInvalidMessages());
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldNotFilterProtobufMessagesWhenEmptyJSONSchema() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToValidMessages(message2);
        List<Message> inputMessages = Arrays.asList(message1, message2);
        FilteredMessages filteredMessages = jsonFilter.filter(inputMessages);
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldNotFilterJsonMessagesWhenEmptyJSONSchema() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToValidMessages(message2);
        List<Message> inputMessages = Arrays.asList(message1, message2);
        FilteredMessages filteredMessages = jsonFilter.filter(inputMessages);
        assertEquals(expectedMessages, filteredMessages);
    }


    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValuesForProtobufMessageType() throws FilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"customer_dynamic_surge_enabled\":{\"const\":\"true\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestBookingLogMessage.class.getName());
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        FilterConfig bookingConsumerConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilter bookingFilter = new JsonFilter(stencilClient, bookingConsumerConfig, firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message);
        FilteredMessages filteredMessages = bookingFilter.filter(Collections.singletonList(message));
        assertEquals(expectedMessages, filteredMessages);
    }


    @Test
    public void shouldThrowExceptionWhenJsonMessageInvalid() throws FilterException {
        Message message1 = new Message(new byte[]{1, 2}, testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse JSON message");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldThrowExceptionWhenProtobufMessageInvalid() throws FilterException {
        Message message1 = new Message(new byte[]{1, 2}, testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse Protobuf message");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldThrowExceptionWhenProtoSchemaClassInvalid() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", "ss");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse Protobuf message");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldLogCauseToFilterOutMessageForProtobufMessageFormat() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        jsonFilter.filter(Arrays.asList(message1, message2));
        verify(firehoseInstrumentation, times(1)).logDebug("Message filtered out due to: {}", "$.order_number: must be a constant value 123");
    }

    @Test
    public void shouldLogCauseToFilterOutMessageForJsonMessageFormat() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        jsonFilter.filter(Arrays.asList(message1, message2));
        verify(firehoseInstrumentation, times(1)).logDebug("Message filtered out due to: {}", "$.order_number: must be a constant value 123");
    }

    @Test
    public void shouldDropProtobufMessageWhenDeserializationFailsAndDropConfigEnabled() throws FilterException {
        Message invalidMessage = new Message(new byte[]{1, 2}, new byte[]{1, 2, 3}, "topic1", 0, 100);
        Message validMessage = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_DROP_DESERIALIZATION_ERROR", "true");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages filteredMessages = jsonFilter.filter(Arrays.asList(invalidMessage, validMessage));
        assertEquals(1, filteredMessages.sizeOfValidMessages());
        assertEquals(1, filteredMessages.sizeOfInvalidMessages());
        verify(firehoseInstrumentation, times(1)).captureCount("firehose_json_filter_deserialization_errors_total", 1L);
        verify(firehoseInstrumentation, times(1)).logWarn(eq("Failed to deserialize protobuf message: {}"), any(String.class));
    }

    @Test
    public void shouldThrowExceptionWhenDeserializationFailsAndDropConfigDisabled() throws FilterException {
        Message invalidMessage = new Message(new byte[]{1, 2}, new byte[]{1, 2, 3}, "topic1", 0, 100);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_DROP_DESERIALIZATION_ERROR", "false");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse Protobuf message");
        jsonFilter.filter(Arrays.asList(invalidMessage));
    }

    @Test
    public void shouldNotDropJsonMessageWhenDeserializationFailsEvenWithDropConfigEnabled() throws FilterException {
        Message invalidMessage = new Message(new byte[]{1, 2}, new byte[]{1, 2, 3}, "topic1", 0, 100);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_DROP_DESERIALIZATION_ERROR", "true");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse JSON message");
        jsonFilter.filter(Arrays.asList(invalidMessage));
    }

    @Test
    public void shouldNotCaptureMetricsWhenDropConfigDisabled() throws FilterException {
        Message invalidMessage = new Message(new byte[]{1, 2}, new byte[]{1, 2, 3}, "topic1", 0, 100);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_DROP_DESERIALIZATION_ERROR", "false");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        try {
            jsonFilter.filter(Arrays.asList(invalidMessage));
        } catch (FilterException e) {
        }
        verify(firehoseInstrumentation, never()).captureCount(eq("firehose_json_filter_deserialization_errors_total"), any(Long.class));
        verify(firehoseInstrumentation, never()).logWarn(eq("Failed to deserialize protobuf message: {}"), any(String.class));
    }

    @Test
    public void shouldDropMultipleInvalidProtobufMessagesAndCaptureCorrectMetrics() throws FilterException {
        Message invalidMessage1 = new Message(new byte[]{1, 2}, new byte[]{1, 2, 3}, "topic1", 0, 100);
        Message invalidMessage2 = new Message(new byte[]{4, 5}, new byte[]{4, 5, 6}, "topic1", 0, 101);
        Message validMessage = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 102);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_DROP_DESERIALIZATION_ERROR", "true");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(stencilClient, filterConfig, firehoseInstrumentation);
        FilteredMessages filteredMessages = jsonFilter.filter(Arrays.asList(invalidMessage1, invalidMessage2, validMessage));
        assertEquals(1, filteredMessages.sizeOfValidMessages());
        assertEquals(2, filteredMessages.sizeOfInvalidMessages());
        verify(firehoseInstrumentation, times(2)).captureCount("firehose_json_filter_deserialization_errors_total", 1L);
        verify(firehoseInstrumentation, times(2)).logWarn(eq("Failed to deserialize protobuf message: {}"), any(String.class));
    }
}
