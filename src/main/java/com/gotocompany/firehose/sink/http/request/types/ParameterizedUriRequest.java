package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.create.IndividualRequestCreator;
import com.gotocompany.firehose.sink.http.request.create.RequestCreator;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * The type Parameterized uri request.
 */
public class ParameterizedUriRequest implements Request {

    /** Reporter used to instrument the request creator. */
    private StatsDReporter statsDReporter;
    /** Bound HTTP sink configuration. */
    private HttpSinkConfig httpSinkConfig;
    /** Serializer that renders each message into the request body. */
    private JsonBody body;
    /** HTTP method used for the requests. */
    private HttpSinkRequestMethodType method;
    /** Builder that wraps the serialized body into an HTTP entity. */
    private RequestEntityBuilder requestEntityBuilder;
    /** Creator that assembles one request per message. */
    private RequestCreator requestCreator;
    /** Mapper that extracts the configured proto fields used as URI parameters. */
    private ProtoToFieldMapper protoToFieldMapper;

    /**
     * Instantiates a new Parameterized uri request.
     *
     * @param statsDReporter     the stats d reporter
     * @param httpSinkConfig     the http sink config
     * @param body               the body
     * @param method             the method
     * @param protoToFieldMapper the proto to field mapper
     */
    public ParameterizedUriRequest(StatsDReporter statsDReporter,
                                   HttpSinkConfig httpSinkConfig,
                                   JsonBody body,
                                   HttpSinkRequestMethodType method,
                                   ProtoToFieldMapper protoToFieldMapper) {
        this.statsDReporter = statsDReporter;
        this.httpSinkConfig = httpSinkConfig;
        this.body = body;
        this.method = method;
        this.protoToFieldMapper = protoToFieldMapper;
    }

    /**
     * Builds one request per message, appending the extracted parameters to each URI.
     *
     * @param messages the messages to convert into requests
     * @return the list of requests to send
     * @throws URISyntaxException    if a service URL cannot be parsed into a URI
     * @throws DeserializerException if a message body cannot be serialized
     */
    @Override
    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws URISyntaxException, DeserializerException {
        return requestCreator.create(messages, requestEntityBuilder.setWrapping(!isTemplateBody(httpSinkConfig)));
    }

    /**
     * Sets request strategy.
     *
     * @param headerBuilder        the header builder
     * @param uriBuilder           the uri builder
     * @param requestEntitybuilder the request entitybuilder
     * @return the request strategy
     */
    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        this.requestCreator = new IndividualRequestCreator(
                new FirehoseInstrumentation(statsDReporter, IndividualRequestCreator.class),
                uriBuilder.withParameterizedURI(protoToFieldMapper, httpSinkConfig.getSinkHttpParameterSource()),
                headerBuilder, method, body, httpSinkConfig);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    /**
     * Reports whether this strategy applies to the current configuration.
     *
     * @return {@code true} when a parameter source is enabled and parameters are placed in the query string
     */
    @Override
    public boolean canProcess() {
        return httpSinkConfig.getSinkHttpParameterSource() != HttpSinkParameterSourceType.DISABLED
                && httpSinkConfig.getSinkHttpParameterPlacement() == HttpSinkParameterPlacementType.QUERY;
    }
}
