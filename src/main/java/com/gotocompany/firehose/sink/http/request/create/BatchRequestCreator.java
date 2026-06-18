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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link RequestCreator} that builds a single HTTP request for the whole batch.
 *
 * <p>It renders one URI and one set of headers, serializes all messages into a single JSON array body and
 * returns a one-element list containing that request. The body is omitted for {@code DELETE} requests when
 * delete bodies are disabled in the configuration. Used by
 * {@link com.gotocompany.firehose.sink.http.request.types.SimpleRequest} when no JSON body template is
 * configured.
 */
public class BatchRequestCreator implements RequestCreator {

    /** Builder that produces the request URI shared by the batch. */
    private UriBuilder uriBuilder;
    /** Builder that produces the headers shared by the batch. */
    private HeaderBuilder headerBuilder;
    /** HTTP method used for the request. */
    private HttpSinkRequestMethodType method;
    /** Serializer that renders the messages into the request body. */
    private JsonBody jsonBody;
    /** Instrumentation used to log the request at debug level. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Bound HTTP sink configuration, consulted for delete-body handling. */
    private HttpSinkConfig httpSinkConfig;

    /**
     * Creates a batch request creator with the builders and configuration it needs.
     *
     * @param firehoseInstrumentation instrumentation used to log the request
     * @param uriBuilder              builder that produces the request URI
     * @param headerBuilder           builder that produces request headers
     * @param method                  the HTTP method to use
     * @param jsonBody                serializer that renders the messages into the request body
     * @param httpSinkConfig          the bound HTTP sink configuration
     */
    public BatchRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.httpSinkConfig = httpSinkConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Builds a single request whose body is the serialized batch of messages.
     *
     * <p>The body is attached for every method except {@code DELETE} when delete bodies are disabled in the
     * configuration, in which case the request is sent without an entity.
     *
     * @param messages             the messages serialized into the single request body
     * @param requestEntityBuilder builder that wraps the serialized batch into an HTTP entity
     * @return a single-element list containing the batch request
     * @throws URISyntaxException if the request URI cannot be built
     */
    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String messagesString = jsonBody.serialize(messages).toString();

        if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
            request.setEntity(requestEntityBuilder.buildHttpEntity(messagesString));
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    uri, headerMap, jsonBody.serialize(messages), method);
        } else {
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                    uri, headerMap, method);
        }
        return Collections.singletonList(request);
    }
}
