package com.gotocompany.firehose.sink.http.request.header;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Header builder for http requests.
 */
public class HeaderBuilder {

    /** Comma-separated {@code key:value} header configuration. */
    private String headerConfig;
    /** Mapper supplying parameterized headers, or {@code null} when not parameterized. */
    private ProtoToFieldMapper protoToFieldMapper;
    /** Source of the parameter values (message key or body) for parameterized headers. */
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

    /**
     * Instantiates a new Header builder.
     *
     * @param headerConfig the header config
     */
    public HeaderBuilder(String headerConfig) {
        this.headerConfig = headerConfig;
    }

    /**
     * Parses the base headers from the configuration string.
     *
     * @return a map of the configured base headers
     */
    public Map<String, String> build() {
        return Arrays.stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
                        .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }

    /**
     * Builds the headers for a single message, merging parameterized headers over the base headers.
     *
     * @param message the message whose payload supplies the parameterized header values
     * @return the combined header map
     */
    public Map<String, String> build(Message message) {
        Map<String, String> baseHeaders = build();
        if (protoToFieldMapper == null) {
            return baseHeaders;
        }

        // flow for parameterized headers
        Map<String, Object> paramMap = protoToFieldMapper
                .getFields((httpSinkParameterSourceType == HttpSinkParameterSourceType.KEY) ? message.getLogKey()
                        : message.getLogMessage());

        Map<String, String> parameterizedHeaders = paramMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        baseHeaders.putAll(parameterizedHeaders);
        return baseHeaders;
    }

    /**
     * Enables parameterized headers by supplying the field mapper and parameter source.
     *
     * @param protoToFieldmapper      mapper that extracts the proto fields added as headers
     * @param httpSinkParameterSource source of the parameter values (message key or body)
     * @return this builder, configured for parameterized headers
     */
    public HeaderBuilder withParameterizedHeader(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }
}
