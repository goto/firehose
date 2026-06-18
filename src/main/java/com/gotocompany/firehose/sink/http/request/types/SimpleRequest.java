package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.create.BatchRequestCreator;
import com.gotocompany.firehose.sink.http.request.create.IndividualRequestCreator;
import com.gotocompany.firehose.sink.http.request.create.RequestCreator;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Simple request.
 */
public class SimpleRequest implements Request {

    /** Bound HTTP sink configuration. */
    private HttpSinkConfig httpSinkConfig;
    /** Serializer that renders each message into the request body. */
    private JsonBody body;
    /** HTTP method used for the requests. */
    private HttpSinkRequestMethodType method;
    /** Builder that wraps the serialized body into an HTTP entity. */
    private RequestEntityBuilder requestEntityBuilder;
    /** Creator that assembles the requests, chosen when the strategy is initialised. */
    private RequestCreator requestCreator;
    /** Reporter used to instrument the request creator. */
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Simple request.
     *
     * @param statsDReporter the stats d reporter
     * @param config         the config
     * @param body           the body
     * @param method         the method
     */
    public SimpleRequest(StatsDReporter statsDReporter, HttpSinkConfig config, JsonBody body, HttpSinkRequestMethodType method) {
        this.httpSinkConfig = config;
        this.body = body;
        this.method = method;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Builds the requests for the batch using the configured request creator.
     *
     * @param messages the messages to convert into requests
     * @return the list of requests to send
     * @throws DeserializerException if a message body cannot be serialized
     * @throws URISyntaxException    if the service URL cannot be parsed into a URI
     */
    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(messages, requestEntityBuilder);
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
        if (isTemplateBody(httpSinkConfig)) {
            this.requestCreator = new IndividualRequestCreator(new FirehoseInstrumentation(
                    statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        } else {
            this.requestCreator = new BatchRequestCreator(new FirehoseInstrumentation(
                    statsDReporter, BatchRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        }
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    /**
     * Reports whether this strategy applies to the current configuration.
     *
     * @return {@code true} when no parameter source is configured and the service URL is static
     */
    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getSinkHttpServiceUrl().contains(",");
        return httpSinkConfig.getSinkHttpParameterSource() == HttpSinkParameterSourceType.DISABLED && !isDynamicUrl;
    }
}
