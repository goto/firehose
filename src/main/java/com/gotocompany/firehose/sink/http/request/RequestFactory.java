package com.gotocompany.firehose.sink.http.request;


import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.gotocompany.firehose.serializer.MessageSerializer;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.firehose.sink.http.request.types.ParameterizedHeaderRequest;
import com.gotocompany.firehose.sink.http.request.types.ParameterizedUriRequest;
import com.gotocompany.firehose.sink.http.request.types.Request;
import com.gotocompany.firehose.sink.http.request.types.SimpleRequest;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.http.factory.SerializerFactory;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.types.DynamicUrlRequest;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import com.gotocompany.firehose.sink.http.request.uri.UriParser;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * Request factory create requests based on configuration.
 */
public class RequestFactory {

    /** Bound HTTP sink configuration that drives strategy selection. */
    private HttpSinkConfig httpSinkConfig;
    /** Parser used to render service URLs from message contents. */
    private UriParser uriParser;
    /** Stencil client used to resolve protobuf parsers for templating. */
    private StencilClient stencilClient;
    /** Reporter passed to the strategies and their request creators for instrumentation. */
    private StatsDReporter statsDReporter;
    /** Instrumentation used to log the selected request type. */
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Request factory.
     *
     * @param statsDReporter the statsd reporter
     * @param httpSinkConfig the http sink config
     * @param stencilClient  the stencil client
     * @param uriParser      the uri parser
     */
    public RequestFactory(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        firehoseInstrumentation = new FirehoseInstrumentation(this.statsDReporter, RequestFactory.class);
    }

    /**
     * Builds the request strategy that matches the current configuration.
     *
     * <p>Evaluates the candidate strategies ({@link SimpleRequest}, {@link DynamicUrlRequest},
     * {@link ParameterizedHeaderRequest} and {@link ParameterizedUriRequest}) in priority order and returns
     * the first that can process the configuration, defaulting to a {@link SimpleRequest}; the chosen strategy
     * is initialised with the header, URI and request-entity builders before being returned.
     *
     * @return the configured request strategy
     */
    public Request createRequest() {
        JsonBody body = createBody();
        HttpSinkRequestMethodType httpSinkRequestMethodType = httpSinkConfig.getSinkHttpRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getSinkHttpHeaders());
        UriBuilder uriBuilder = new UriBuilder(httpSinkConfig.getSinkHttpServiceUrl(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        List<Request> requests = Arrays.asList(
                new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new DynamicUrlRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()),
                new ParameterizedUriRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()));

        Request request = requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType));
        firehoseInstrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    /**
     * Builds the mapper that extracts configured proto fields for parameterized URIs and headers.
     *
     * @return a proto-to-field mapper for the parameter source schema
     */
    private ProtoToFieldMapper getProtoToFieldMapper() {
        Parser protoParser = stencilClient.getParser(httpSinkConfig.getSinkHttpParameterSchemaProtoClass());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getInputSchemaProtoToColumnMapping());
    }

    /**
     * Builds the JSON body serializer wrapper used by the request strategies.
     *
     * @return the request body serializer
     */
    private JsonBody createBody() {
        MessageSerializer messageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient,
                statsDReporter)
                .build();
        return new JsonBody(messageSerializer);
    }
}
