package com.gotocompany.firehose.sink.http.request.uri;


import com.gotocompany.firehose.message.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.stencil.Parser;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Arrays;
import java.util.List;

/**
 * URI parser for http requests.
 */
public class UriParser {
    /** Stencil parser used to decode the message payload into a dynamic message. */
    private Parser protoParser;
    /** Selects the payload source; {@code "key"} uses the message key, otherwise the message body. */
    private String parserMode;

    /**
     * Creates a URI parser bound to a proto parser and payload selection mode.
     *
     * @param protoParser the Stencil parser used to decode the payload
     * @param parserMode  the payload source, {@code "key"} or {@code "message"}
     */
    public UriParser(Parser protoParser, String parserMode) {
        this.protoParser = protoParser;
        this.parserMode = parserMode;
    }

    /**
     * Renders the service URL for a single message.
     *
     * @param message    the message whose payload supplies the substitution values
     * @param serviceUrl the URL pattern, optionally followed by comma-separated proto field numbers
     * @return the rendered URL
     * @throws IllegalArgumentException if the payload cannot be parsed or the URL is invalid
     */
    public String parse(Message message, String serviceUrl) {
        DynamicMessage parsedMessage = parseEsbMessage(message);
        return parseServiceUrl(parsedMessage, serviceUrl);

    }

    /**
     * Parses the message payload into a dynamic protobuf message.
     *
     * @param message the message to parse
     * @return the parsed dynamic message
     * @throws IllegalArgumentException if the payload is not valid protobuf
     */
    private DynamicMessage parseEsbMessage(Message message) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(message));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Unable to parse Service URL", e);
        }
        return parsedMessage;
    }

    /**
     * Splits the service URL into its pattern and field-number variables and renders it.
     *
     * @param data       the parsed message supplying the substitution values
     * @param serviceUrl the raw service URL configuration
     * @return the rendered URL, or the pattern itself when no variables are present
     * @throws IllegalArgumentException if the service URL is empty
     */
    private String parseServiceUrl(DynamicMessage data, String serviceUrl) {
        if (StringUtils.isEmpty(serviceUrl)) {
            throw new IllegalArgumentException("Service URL '" + serviceUrl + "' is invalid");
        }
        String[] urlStrings = serviceUrl.split(",");
        if (urlStrings.length == 0) {
            throw new InvalidConfigurationException("Empty Service URL configuration: '" + serviceUrl + "'");
        }
        urlStrings = Arrays
                .stream(urlStrings)
                .map(String::trim)
                .toArray(String[]::new);

        String urlPattern = urlStrings[0];
        String urlVariables = StringUtils.join(Arrays.copyOfRange(urlStrings, 1, urlStrings.length), ",");
        String renderedUrl = renderStringUrl(data, urlPattern, urlVariables);
        return StringUtils.isEmpty(urlVariables)
                ? urlPattern
                : renderedUrl;
    }

    /**
     * Substitutes the proto field values into the URL pattern.
     *
     * @param parsedMessage    the parsed message supplying the values
     * @param pattern          the URL format pattern
     * @param patternVariables comma-separated proto field numbers to substitute
     * @return the formatted URL, or the pattern unchanged when there are no variables
     */
    private String renderStringUrl(DynamicMessage parsedMessage, String pattern, String patternVariables) {
        if (StringUtils.isEmpty(patternVariables)) {
            return pattern;
        }
        List<String> patternVariableFieldNumbers = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNumbers
                .stream()
                .map(fieldNumber -> getDataByFieldNumber(parsedMessage, fieldNumber))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    /**
     * Reads a single proto field value by its field number.
     *
     * @param parsedMessage the parsed message to read from
     * @param fieldNumber   the proto field number as a string
     * @return the field value
     * @throws IllegalArgumentException if the field number is not numeric or no such field exists
     */
    private Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
        int fieldNumberInt;
        try {
            fieldNumberInt = Integer.parseInt(fieldNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Proto Index");
        }
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
        }
        return parsedMessage.getField(fieldDescriptor);
    }

    /**
     * Returns the raw payload bytes for the configured parser mode.
     *
     * @param message the message to read
     * @return the message key when the mode is {@code "key"}, otherwise the message body
     */
    private byte[] getPayload(Message message) {
        if (parserMode.equals("key")) {
            return message.getLogKey();
        } else {
            return message.getLogMessage();
        }
    }

}
