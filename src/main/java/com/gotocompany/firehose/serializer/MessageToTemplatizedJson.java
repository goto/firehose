package com.gotocompany.firehose.serializer;


import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.google.gson.Gson;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.gotocompany.stencil.Parser;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts kafka messages into Templatized json.
 */
public class MessageToTemplatizedJson implements MessageSerializer {
    /** Regex matching quoted JSON-path placeholders such as {@code "$.field"} in the template. */
    private static final String TEMPLATE_PATH_REGEX = "\"\\$\\.[^\\s\\\\]*?\"";
    /** Placeholder that expands to the entire message JSON. */
    private static final String ALL_FIELDS_FROM_TEMPLATE = "\"$._all_\"";
    /** The JSON body template with JSON-path placeholders to fill in. */
    private final String httpSinkJsonBodyTemplate;
    /** Gson used to render extracted values as JSON. */
    private final Gson gson;
    /** Stencil parser used to decode the protobuf message. */
    private Parser protoParser;
    /** The set of placeholder paths discovered in the template. */
    private HashSet<String> pathsToReplace;
    /** Parser used to validate that the template is well-formed JSON. */
    private JSONParser jsonParser;
    /** JSON-path configuration controlling missing-path behaviour. */
    private Configuration jsonPathConfig;
    /** Records debug logs and warnings about missing paths. */
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a templatized serializer and validates the template, pre-extracting its paths.
     *
     * @param firehoseInstrumentation  the instrumentation used for logging
     * @param httpSinkJsonBodyTemplate the JSON body template containing JSON-path placeholders
     * @param protoParser              the parser used to decode the protobuf message
     * @param option                   an optional JSON-path option, or {@code null} for defaults
     * @return a ready-to-use serializer
     * @throws ConfigurationException if the template is not valid JSON
     */
    public static MessageToTemplatizedJson create(FirehoseInstrumentation firehoseInstrumentation, String httpSinkJsonBodyTemplate, Parser protoParser, Option option) {
        MessageToTemplatizedJson messageToTemplatizedJson = new MessageToTemplatizedJson(firehoseInstrumentation, httpSinkJsonBodyTemplate, protoParser, option);
        if (messageToTemplatizedJson.isInvalidJson()) {
            throw new ConfigurationException("Given HTTPSink JSON body template: " + httpSinkJsonBodyTemplate + " must be a valid JSON.");
        }
        messageToTemplatizedJson.setPathsFromTemplate();
        return messageToTemplatizedJson;
    }

    /**
     * Creates a templatized serializer without validating the template.
     *
     * <p>Prefer {@link #create} which also validates the template and extracts its placeholder paths.
     *
     * @param firehoseInstrumentation  the instrumentation used for logging
     * @param httpSinkJsonBodyTemplate the JSON body template containing JSON-path placeholders
     * @param protoParser              the parser used to decode the protobuf message
     * @param option                   an optional JSON-path option, or {@code null} for defaults
     */
    public MessageToTemplatizedJson(FirehoseInstrumentation firehoseInstrumentation, String httpSinkJsonBodyTemplate, Parser protoParser, Option option) {
        this.httpSinkJsonBodyTemplate = httpSinkJsonBodyTemplate;
        this.protoParser = protoParser;
        this.jsonParser = new JSONParser();
        this.gson = new Gson();
        this.jsonPathConfig = option == null
                ? Configuration.defaultConfiguration()
                : Configuration.defaultConfiguration().addOptions(option);
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Scans the template for JSON-path placeholders and records them for substitution.
     */
    private void setPathsFromTemplate() {
        HashSet<String> paths = new HashSet<>();
        Pattern pattern = Pattern.compile(TEMPLATE_PATH_REGEX);
        Matcher matcher = pattern.matcher(httpSinkJsonBodyTemplate);
        while (matcher.find()) {
            paths.add(matcher.group(0));
        }
        List<String> pathList = new ArrayList<>(paths);
        firehoseInstrumentation.logDebug("\nPaths: {}", pathList);
        this.pathsToReplace = paths;
    }

    /**
     * Create json string from kafka message based on json Template.
     *
     * @param message the message
     * @return the string
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public String serialize(Message message) throws DeserializerException {
        try {
            String jsonMessage;
            String jsonString;
            // only supports messages not keys
            DynamicMessage msg = protoParser.parse(message.getLogMessage());
            jsonMessage = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames().print(msg);
            String finalMessage = httpSinkJsonBodyTemplate;

            for (String path : pathsToReplace) {
                if (path.equals(ALL_FIELDS_FROM_TEMPLATE)) {
                    jsonString = jsonMessage;
                } else {
                    Object element = JsonPath.using(jsonPathConfig).parse(jsonMessage).read(path.replaceAll("\"", ""));
                    if (element == null && (jsonPathConfig.getOptions().contains(Option.DEFAULT_PATH_LEAF_TO_NULL)
                            || jsonPathConfig.getOptions().contains(Option.SUPPRESS_EXCEPTIONS))) {
                        firehoseInstrumentation.logWarn("Missing value for path: {}", path);
                        jsonString = "";
                    } else {
                        jsonString = gson.toJson(element);
                    }
                }
                finalMessage = finalMessage.replace(path, jsonString);
            }

            return finalMessage;
        } catch (InvalidProtocolBufferException | PathNotFoundException e) {
            throw new DeserializerException(e.getMessage());
        }
    }

    /**
     * Returns whether the configured template fails to parse as JSON.
     *
     * @return {@code true} if the template is not valid JSON
     */
    private boolean isInvalidJson() {
        try {
            jsonParser.parse(httpSinkJsonBodyTemplate);
        } catch (ParseException e) {
            return true;
        }
        return false;
    }
}
