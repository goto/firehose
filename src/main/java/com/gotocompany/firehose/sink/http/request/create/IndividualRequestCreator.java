package com.gotocompany.firehose.sink.http.request.create;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.firehose.sink.http.request.HttpRequestMethodFactory;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link RequestCreator} that builds one HTTP request per message.
 *
 * <p>For each message it renders the URI (optionally parameterized), builds the headers, creates the
 * method-specific request via {@link com.gotocompany.firehose.sink.http.request.HttpRequestMethodFactory}
 * and attaches the serialized body as the entity. The body is omitted for {@code DELETE} requests when
 * delete bodies are disabled in the configuration. Used by the per-message
 * {@link com.gotocompany.firehose.sink.http.request.types.Request} strategies.
 */
public class IndividualRequestCreator implements RequestCreator {

    /** Builder that produces the headers for each request. */
    private HeaderBuilder headerBuilder;
    /** Serializer that renders each message into the request body. */
    private JsonBody jsonBody;
    /** HTTP method used for the requests. */
    private HttpSinkRequestMethodType method;
    /** Builder that produces the per-message request URI. */
    private UriBuilder uriBuilder;
    /** Instrumentation used to log each request at debug level. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Bound HTTP sink configuration, consulted for delete-body handling. */
    private HttpSinkConfig httpSinkConfig;

    /**
     * Creates an individual request creator with the builders and configuration it needs.
     *
     * @param firehoseInstrumentation instrumentation used to log requests
     * @param uriBuilder              builder that produces the per-message request URI
     * @param headerBuilder           builder that produces request headers
     * @param method                  the HTTP method to use
     * @param body                    serializer that renders each message into the request body
     * @param httpSinkConfig          the bound HTTP sink configuration
     */
    public IndividualRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody body, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.jsonBody = body;
        this.method = method;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.httpSinkConfig = httpSinkConfig;
    }

    /**
     * Builds one request per message, rendering its URI, headers and body.
     *
     * <p>The body is attached for every method except {@code DELETE} when delete bodies are disabled in the
     * configuration, in which case the request is sent without an entity.
     *
     * @param messages the messages to convert into requests
     * @param entity   builder that wraps each serialized body into an HTTP entity
     * @return one request per message
     * @throws URISyntaxException if a request URI cannot be built
     */
    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder entity) throws URISyntaxException {
        List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
        List<String> bodyContents = jsonBody.serialize(messages);
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            URI requestUrl = uriBuilder.build(message);
            HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(requestUrl, method);

            Map<String, String> headerMap = headerBuilder.build(message);
            headerMap.forEach(request::addHeader);
            if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
                request.setEntity(entity.buildHttpEntity(bodyContents.get(i)));

                firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                        requestUrl, headerMap, bodyContents.get(i), method);
            } else {
                firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                        requestUrl, headerMap, method);
            }
            requests.add(request);
        }
        return requests;
    }
}
