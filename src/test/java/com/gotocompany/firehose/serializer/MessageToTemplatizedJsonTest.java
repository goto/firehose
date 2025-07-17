package com.gotocompany.firehose.serializer;




import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.consumer.TestAggregatedSupplyMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import com.jayway.jsonpath.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.MockitoAnnotations.initMocks;

public class MessageToTemplatizedJsonTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private Parser protoParser;

    private String logMessage;
    private String logKey;

    @Before
    public void setup() {
        initMocks(this);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithSingleUnknownField() {
        String template = "{\"test\":\"$.vehicle_type\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(firehoseInstrumentation, template, protoParser, null);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\"test\":\"BIKE\"}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithAllOptionConfigurationWhenFieldsExist() {
        Option[] options = {Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS, Option.REQUIRE_PROPERTIES};
        for (Option option : options) {
            String template = "{\"test\":\"$.vehicle_type\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                    .create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            String serializedMessage = messageToTemplatizedJson.serialize(message);
            String expectedMessage = "{\"test\":\"BIKE\"}";
            Assert.assertEquals(expectedMessage, serializedMessage);
        }
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateAsListWithAlwaysReturnList() {
        Option[] options = {Option.ALWAYS_RETURN_LIST};
        for (Option option : options) {
            String template = "{\"test\":\"$.vehicle_type\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                    .create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            String serializedMessage = messageToTemplatizedJson.serialize(message);
            //  Forces all path evaluations to return a list even when only a single value is found
            //  Helps standardize return types in your application
            String expectedMessage = "{\"test\":[\"BIKE\"]}";
            Assert.assertEquals(expectedMessage, serializedMessage);
        }
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateAsListOfPathWhenTheFieldIsFoundWithAsPathList() {
        Option[] options = {Option.AS_PATH_LIST};
        for (Option option : options) {
            String template = "{\"test\":\"$.vehicle_type\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                    .create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            String serializedMessage = messageToTemplatizedJson.serialize(message);
            //  Note: The serialized message will contain the path as a string in the list
            //  Returns path references instead of actual values
            //  Useful when you need to know the paths where matches were found rather than the values
            String expectedMessage = "{\"test\":[\"$[\\u0027vehicle_type\\u0027]\"]}";
            Assert.assertEquals(expectedMessage, serializedMessage);
        }
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithConfigSuppressExceptions() {
        String template = "{\"test\":\"$.vehicle_type\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(firehoseInstrumentation, template, protoParser, Option.SUPPRESS_EXCEPTIONS);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\"test\":\"BIKE\"}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldGiveEmptyFieldInMessageWithSuppressExceptionsWhenFieldDoesNotExistAndMapIsUsed() {
        String template = "{\"test\":\"$.vehicle_type\", \"test2\":\"$.xyz\", \"test3\":\"$.field1.value1.field2.value2\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(firehoseInstrumentation, template, protoParser, Option.SUPPRESS_EXCEPTIONS);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\"test\":\"BIKE\", \"test2\":, \"test3\":}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldGiveEmptyFieldInMessageWithDefaultPathLeafToNullWhenFieldDoesNotExist() {
        Option[] options = {Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS};
        for (Option option : options) {
            String template = "{\"test\":\"$.vehicle_type\", \"test2\":\"$.xyz\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                    .create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            String serializedMessage = messageToTemplatizedJson.serialize(message);
            String expectedMessage = "{\"test\":\"BIKE\", \"test2\":}";
            Assert.assertEquals(expectedMessage, serializedMessage);
        }
    }

    @Test
    public void shouldFailWithNoPathFoundExceptionWithDefaultPathLeafToNullWhenMapFieldDoesNotExist() {
        Option[] options = {Option.DEFAULT_PATH_LEAF_TO_NULL};
        for (Option option : options) {
            expectedException.expect(DeserializerException.class);
            expectedException.expectMessage("Missing property in path $['field1']");
            String template = "{\"test\":\"$.vehicle_type\", \"test2\":\"$.xyz\", \"test3\":\"$.field1.value1.field2.value2\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                    .create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            String serializedMessage = messageToTemplatizedJson.serialize(message);

        }
    }

    @Test
    public void shouldFailWithDeserializerExceptionForListOptionsWhenFieldDoesNotExist() {
        Option[] options = {Option.ALWAYS_RETURN_LIST, Option.AS_PATH_LIST, Option.REQUIRE_PROPERTIES};
        for (Option option : options) {
            expectedException.expect(DeserializerException.class);
            expectedException.expectMessage("No results for path: $['xyz']");
            String template = "{\"test\":\"$.vehicle_type\", \"test2\":\"$.xyz\"}";
            StencilClient stencilClient = StencilClientFactory.getClient();
            protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
            MessageToTemplatizedJson messageToTemplatizedJson =
                    MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, option);
            Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                    Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

            messageToTemplatizedJson.serialize(message);
        }
    }

    @Test
    public void shouldFailWithIllegalArgumentExceptionWithBadConfigHttpJsonBodyTemplateParseOptionConverter() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("No enum constant com.jayway.jsonpath.Option.abc");

        String template = "{\"test\":\"$.vehicle_type\", \"test2\":\"$.xyz\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(firehoseInstrumentation, template, protoParser, Option.valueOf("abc"));
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messageToTemplatizedJson.serialize(message);
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithAsItIs() {
        String template = "\"$._all_\"";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, null);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\n"
                + "  \"window_start_time\": \"2017-03-20T10:54:00Z\",\n"
                + "  \"window_end_time\": \"2017-03-20T10:55:00Z\",\n"
                + "  \"s2_id_level\": 13,\n"
                + "  \"s2_id\": \"3344472187078705152\",\n"
                + "  \"vehicle_type\": \"BIKE\",\n"
                + "  \"unique_drivers\": \"3\"\n"
                + "}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldThrowIfNoPathsFoundInTheProto() {
        expectedException.expect(DeserializerException.class);
        expectedException.expectMessage("No results for path: $['invalidPath']");

        String template = "{\"test\":\"$.invalidPath\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, null);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messageToTemplatizedJson.serialize(message);
    }

    @Test
    public void shouldFailForNonJsonTemplate() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, null);
    }


    @Test
    public void shouldDoRegexMatchingToReplaceThingsFromProtobuf() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, null);
    }

    @Test
    public void shouldLogPaths() {
        HashSet<String> paths = new HashSet<>();
        String template = "\"$._all_\"";
        String templatePathRegex = "\"\\$\\.[^\\s\\\\]*?\"";

        Pattern pattern = Pattern.compile(templatePathRegex);
        Matcher matcher = pattern.matcher(template);
        while (matcher.find()) {
            paths.add(matcher.group(0));
        }
        List<String> pathList = new ArrayList<>(paths);

        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = stencilClient.getParser(TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson.create(firehoseInstrumentation, template, protoParser, null);

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logDebug("\nPaths: {}", pathList);
    }
}
