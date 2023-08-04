package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.enums.InputSchemaType;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.consumer.TestAggregatedSupplyMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class MessageToJsonTest {
    private String logMessage;
    private String logKey;
    private String logMessageJSONString;
    private String logKeyJSONString;
    private Parser protoParser;

    @Before
    public void setUp() {
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";

        logMessageJSONString = "{\n"
                + "    \"uniqueDrivers\": \"3\",\n"
                + "    \"windowStartTime\": \"Mar 20, 2017 10:54:00 AM\",\n"
                + "    \"windowEndTime\": \"Mar 20, 2017 10:55:00 AM\",\n"
                + "    \"s2IdLevel\": 13,\n"
                + "    \"vehicleType\": \"BIKE\",\n"
                + "    \"s2Id\": \"3344472187078705152\"\n"
                + "  }";
        logKeyJSONString = "sample-key1";
    }

    public byte[] stringToByteArray(String inputString) {
        return StandardCharsets.UTF_8.encode(inputString).array();
    }

    @Test
    public void shouldProperlySerializeEsbMessage() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
        String actualOutput = messageToJson.serialize(message);
        assertEquals(actualOutput, "{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\",\"logKey\":\"{"
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\"}");
    }


    @Test
    public void shouldProperlySerializeJsonInputMessage() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);
        Message message = new Message(logKeyJSONString.getBytes(), logMessageJSONString.getBytes(), "sample-topic", 0, 100);
        message.setInputSchemaType(InputSchemaType.JSON);

        String expectedOutput = "{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\","
                + "\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\",\\\"s2Id\\\":\\\"3344472187078705152\\\"}\","
                + "\"topic\":\"sample-topic\",\"logKey\":\"sample-key1\"}";

        String actualOutput = messageToJson.serialize(message);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void shouldSerializeWhenKeyIsMissing() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0,
                100);
        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldSerializeJSONInputMessageWhenKeyIsMissing() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);
        Message message = new Message(null, logMessageJSONString.getBytes(), "sample-topic", 0, 100);
        message.setInputSchemaType(InputSchemaType.JSON);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);


    }

    @Test
    public void shouldSerializeWhenKeyIsEmptyWithTimestampsAsSimpleDateFormatWhenFlagIsEnabled() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldSerializeWhenKeyIsEmptyWithTimestampsAsISOFormatWhenFlagIsDisabled() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, false);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"2017-03-20T10:54:00Z\\\","
                + "\\\"windowEndTime\\\":\\\"2017-03-20T10:55:00Z\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldWrappedSerializedJsonInArrayWithTimestampsAsSimpleDateFormatWhenFlagsAreEnabled() throws DeserializerException {
        boolean wrappedInsideArray = true;
        MessageToJson messageToJson = new MessageToJson(protoParser, false, wrappedInsideArray, true);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("[{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}]", actualOutput);
    }

    @Test
    public void shouldReturnTheTimestampFieldsInISOFormatIfSimpleDateFormatIsDisabled() throws DeserializerException {
        boolean wrappedInsideArray = true;
        MessageToJson messageToJson = new MessageToJson(protoParser, false, wrappedInsideArray, false);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("[{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"2017-03-20T10:54:00Z\\\","
                + "\\\"windowEndTime\\\":\\\"2017-03-20T10:55:00Z\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}]", actualOutput);
    }
}


