package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.create.IndividualRequestCreator;
import com.gotocompany.firehose.sink.http.request.create.RequestCreator;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Dynamic url request.
 */
public class DynamicUrlRequest implements Request {

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

    /**
     * Instantiates a new Dynamic url request.
     *
     * @param statsDReporter the stats d reporter
     * @param httpSinkConfig the http sink config
     * @param body           the body
     * @param method         the method
     */
    public DynamicUrlRequest(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, JsonBody body, HttpSinkRequestMethodType method) {
        this.statsDReporter = statsDReporter;
        this.httpSinkConfig = httpSinkConfig;
        this.body = body;
        this.method = method;
    }

    /**
     * Builds one request per message, each targeting the URL rendered from that message.
     *
     * @param messages the messages to convert into requests
     * @return the list of requests to send
     * @throws DeserializerException if a message body cannot be serialized
     * @throws URISyntaxException    if a rendered URL cannot be parsed into a URI
     */
    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException {
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
                new FirehoseInstrumentation(statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    /**
     * Reports whether this strategy applies to the current configuration.
     *
     * @return {@code true} when no parameter source is configured and the service URL is a comma-separated template
     */
    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getSinkHttpServiceUrl().contains(",");
        return httpSinkConfig.getSinkHttpParameterSource() == HttpSinkParameterSourceType.DISABLED && isDynamicUrl;
    }
}
