package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;
import com.gotocompany.firehose.config.converter.HttpSinkRequestMethodConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterSourceTypeConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterDataFormatConverter;
import com.gotocompany.firehose.config.converter.HttpJsonBodyTemplateParseOptionConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterPlacementTypeConverter;
import com.gotocompany.firehose.config.converter.HttpSinkSerializerJsonTypecastConfigConverter;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.jayway.jsonpath.Option;

import java.util.Map;
import java.util.function.Function;

/**
 * Owner configuration for the HTTP sink, which posts consumed messages to an HTTP endpoint.
 *
 * <p>It defines the retryable and loggable HTTP status-code ranges, the request method, timeouts and
 * connection limits, the service URL and headers, OAuth2 credentials, the JSON body template and its
 * serialization (including type casts), and the parameterized-request settings that inject message
 * fields into the URL query or headers. Each accessor maps to an environment variable via
 * {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface HttpSinkConfig extends AppConfig {

    /**
     * Returns the HTTP status codes that trigger a retry, set by
     * {@code SINK_HTTP_RETRY_STATUS_CODE_RANGES} as inclusive ranges (defaulting to {@code 400-600})
     * and expanded into a membership map by
     * {@link com.gotocompany.firehose.config.converter.RangeToHashMapConverter}.
     *
     * @return a membership map keyed by retryable HTTP status code
     */
    @Key("SINK_HTTP_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRetryStatusCodeRanges();

    /**
     * Returns the HTTP status codes whose responses are logged, set by
     * {@code SINK_HTTP_REQUEST_LOG_STATUS_CODE_RANGES} as inclusive ranges (defaulting to
     * {@code 400-499}) and expanded into a membership map by
     * {@link com.gotocompany.firehose.config.converter.RangeToHashMapConverter}.
     *
     * @return a membership map keyed by loggable HTTP status code
     */
    @Key("SINK_HTTP_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRequestLogStatusCodeRanges();

    /**
     * Returns the timeout in milliseconds for an HTTP request, set by
     * {@code SINK_HTTP_REQUEST_TIMEOUT_MS} and defaulting to {@code 10000}.
     *
     * @return the HTTP request timeout in milliseconds
     */
    @Key("SINK_HTTP_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkHttpRequestTimeoutMs();

    /**
     * Returns the HTTP method used for requests, set by {@code SINK_HTTP_REQUEST_METHOD}, converted by
     * {@link com.gotocompany.firehose.config.converter.HttpSinkRequestMethodConverter} and defaulting
     * to {@code put}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType}
     */
    @Key("SINK_HTTP_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpSinkRequestMethodConverter.class)
    HttpSinkRequestMethodType getSinkHttpRequestMethod();

    /**
     * Returns the maximum number of concurrent HTTP connections, set by
     * {@code SINK_HTTP_MAX_CONNECTIONS} and defaulting to {@code 10}.
     *
     * @return the maximum HTTP connection count
     */
    @Key("SINK_HTTP_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getSinkHttpMaxConnections();

    /**
     * Returns the HTTP endpoint URL messages are posted to, set by {@code SINK_HTTP_SERVICE_URL}; it
     * may contain templated path elements for dynamic-URL requests.
     *
     * @return the HTTP service URL
     */
    @Key("SINK_HTTP_SERVICE_URL")
    String getSinkHttpServiceUrl();

    /**
     * Returns the static headers attached to every HTTP request, set by {@code SINK_HTTP_HEADERS} as
     * comma-separated {@code key:value} pairs and defaulting to an empty string.
     *
     * @return the configured HTTP request headers
     */
    @Key("SINK_HTTP_HEADERS")
    @DefaultValue("")
    String getSinkHttpHeaders();

    /**
     * Returns the source of the dynamic parameters injected into requests, set by
     * {@code SINK_HTTP_PARAMETER_SOURCE}, converted by
     * {@link com.gotocompany.firehose.config.converter.HttpSinkParameterSourceTypeConverter} and
     * defaulting to {@code disabled} (no parameterization).
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType}
     */
    @Key("SINK_HTTP_PARAMETER_SOURCE")
    @DefaultValue("disabled")
    @ConverterClass(HttpSinkParameterSourceTypeConverter.class)
    HttpSinkParameterSourceType getSinkHttpParameterSource();

    /**
     * Returns how a message is serialized into the request body, set by {@code SINK_HTTP_DATA_FORMAT},
     * converted by
     * {@link com.gotocompany.firehose.config.converter.HttpSinkParameterDataFormatConverter} and
     * defaulting to {@code proto}.
     *
     * @return the configured {@link com.gotocompany.firehose.config.enums.HttpSinkDataFormatType}
     */
    @Key("SINK_HTTP_DATA_FORMAT")
    @DefaultValue("proto")
    @ConverterClass(HttpSinkParameterDataFormatConverter.class)
    HttpSinkDataFormatType getSinkHttpDataFormat();

    /**
     * Indicates whether OAuth2 client-credentials authentication is used for HTTP requests, set by
     * {@code SINK_HTTP_OAUTH2_ENABLE} and defaulting to {@code false}.
     *
     * @return {@code true} if OAuth2 is enabled
     */
    @Key("SINK_HTTP_OAUTH2_ENABLE")
    @DefaultValue("false")
    Boolean isSinkHttpOAuth2Enable();

    /**
     * Returns the OAuth2 token endpoint used to obtain access tokens, set by
     * {@code SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL} and defaulting to {@code https://localhost:8888}.
     *
     * @return the OAuth2 access-token URL
     */
    @Key("SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL")
    @DefaultValue("https://localhost:8888")
    String getSinkHttpOAuth2AccessTokenUrl();

    /**
     * Returns the OAuth2 client id (name) used for the client-credentials grant, set by
     * {@code SINK_HTTP_OAUTH2_CLIENT_NAME} and defaulting to {@code client_name}.
     *
     * @return the OAuth2 client name
     */
    @Key("SINK_HTTP_OAUTH2_CLIENT_NAME")
    @DefaultValue("client_name")
    String getSinkHttpOAuth2ClientName();

    /**
     * Returns the OAuth2 client secret used for the client-credentials grant, set by
     * {@code SINK_HTTP_OAUTH2_CLIENT_SECRET} and defaulting to {@code client_secret}.
     *
     * @return the OAuth2 client secret
     */
    @Key("SINK_HTTP_OAUTH2_CLIENT_SECRET")
    @DefaultValue("client_secret")
    String getSinkHttpOAuth2ClientSecret();

    /**
     * Returns the OAuth2 scope requested when fetching an access token, set by
     * {@code SINK_HTTP_OAUTH2_SCOPE} and defaulting to {@code scope}.
     *
     * @return the OAuth2 scope
     */
    @Key("SINK_HTTP_OAUTH2_SCOPE")
    @DefaultValue("scope")
    String getSinkHttpOAuth2Scope();

    /**
     * Returns the JSON body template applied to each message before sending, set by
     * {@code SINK_HTTP_JSON_BODY_TEMPLATE} and defaulting to an empty string (no templating).
     *
     * @return the HTTP JSON body template
     */
    @Key("SINK_HTTP_JSON_BODY_TEMPLATE")
    @DefaultValue("")
    String getSinkHttpJsonBodyTemplate();

    /**
     * Returns the JSONPath option applied when evaluating the body template, set by
     * {@code SINK_HTTP_JSON_BODY_TEMPLATE_PARSE_OPTION}, converted by
     * {@link com.gotocompany.firehose.config.converter.HttpJsonBodyTemplateParseOptionConverter} and
     * defaulting to an empty value (no option, resolving to {@code null}).
     *
     * @return the configured {@link com.jayway.jsonpath.Option}, or {@code null} when unset
     */
    @Key("SINK_HTTP_JSON_BODY_TEMPLATE_PARSE_OPTION")
    @DefaultValue("")
    @ConverterClass(HttpJsonBodyTemplateParseOptionConverter.class)
    Option getSinkHttpJsonBodyTemplateParseOption();

    /**
     * Returns where dynamic parameters are placed in the request, set by
     * {@code SINK_HTTP_PARAMETER_PLACEMENT}, converted by
     * {@link com.gotocompany.firehose.config.converter.HttpSinkParameterPlacementTypeConverter} and
     * defaulting to {@code header}.
     *
     * @return the configured
     *     {@link com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType}
     */
    @Key("SINK_HTTP_PARAMETER_PLACEMENT")
    @DefaultValue("header")
    @ConverterClass(HttpSinkParameterPlacementTypeConverter.class)
    HttpSinkParameterPlacementType getSinkHttpParameterPlacement();

    /**
     * Returns the fully-qualified protobuf class used to extract dynamic request parameters, set by
     * {@code SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS}.
     *
     * @return the parameter schema proto class name
     */
    @Key("SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS")
    String getSinkHttpParameterSchemaProtoClass();

    /**
     * Indicates whether DELETE requests are allowed to carry a body, set by
     * {@code SINK_HTTP_DELETE_BODY_ENABLE} and defaulting to {@code true}.
     *
     * @return {@code true} if DELETE requests include a body
     */
    @Key("SINK_HTTP_DELETE_BODY_ENABLE")
    @DefaultValue("true")
    Boolean getSinkHttpDeleteBodyEnable();

    /**
     * Indicates whether protobuf timestamps are rendered using a simple date format (rather than the
     * default representation) when serializing the body, set by
     * {@code SINK_HTTP_SIMPLE_DATE_FORMAT_ENABLE} and defaulting to {@code true}.
     *
     * @return {@code true} if the simple date format is used
     */
    @Key("SINK_HTTP_SIMPLE_DATE_FORMAT_ENABLE")
    @DefaultValue("true")
    Boolean getSinkHttpSimpleDateFormatEnable();

    /**
     * Returns the JSONPath type-cast rules applied while serializing the JSON body, set by
     * {@code SINK_HTTP_SERIALIZER_JSON_TYPECAST}, parsed by
     * {@link com.gotocompany.firehose.config.converter.HttpSinkSerializerJsonTypecastConfigConverter}
     * and defaulting to {@code []} (no casts).
     *
     * @return a map keyed by JSONPath whose values cast a string to the configured target type
     */
    @Key("SINK_HTTP_SERIALIZER_JSON_TYPECAST")
    @ConverterClass(HttpSinkSerializerJsonTypecastConfigConverter.class)
    @DefaultValue("[]")
    Map<String, Function<String, Object>> getSinkHttpSerializerJsonTypecast();

}
