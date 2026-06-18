package com.gotocompany.firehose.sink.http;


import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.common.AbstractHttpSink;
import com.gotocompany.firehose.sink.http.request.types.Request;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * HttpSink implement {@link AbstractHttpSink } lifecycle for HTTP.
 */
public class HttpSink extends AbstractHttpSink {

    /** Strategy that converts a batch of messages into concrete HTTP requests. */
    private final Request request;

    /**
     * Instantiates a new Http sink.
     *
     * @param firehoseInstrumentation    the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public HttpSink(FirehoseInstrumentation firehoseInstrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(firehoseInstrumentation, "http", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
        this.request = request;
    }

    /**
     * Prepares the batch for delivery by building one HTTP request per applicable message.
     *
     * <p>The source messages are first recorded by the superclass, then handed to the
     * {@link com.gotocompany.firehose.sink.http.request.types.Request} strategy whose output becomes
     * the list of requests that the inherited execute loop will send.
     *
     * @param messages the batch of messages to be delivered in this push
     * @throws DeserializerException if a message body cannot be serialized
     * @throws IOException           if the service URL is malformed (wraps {@code URISyntaxException}) or another I/O error occurs
     * @throws SQLException          never thrown by this implementation; declared to satisfy the overridden contract
     */
    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        try {
            super.prepare(messages);
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reads the request body back as a list of lines, used when logging requests or counting drops.
     *
     * <p>A {@code DELETE} request that carries no entity yields an empty list; otherwise the entity
     * content is read as UTF-8 and split into individual lines.
     *
     * @param httpRequest the request whose entity content should be read
     * @return the request body split into lines, or an empty list when there is no entity
     * @throws IOException if the entity content cannot be read
     */
    @Override
    protected List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        if (httpRequest.getMethod().equals("DELETE") && httpRequest.getEntity() == null) {
            return new ArrayList<>();
        }
        try (InputStream inputStream = httpRequest.getEntity().getContent()) {
            return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
        }
    }

    /**
     * Records how many messages were dropped because the endpoint returned a non-retryable failure.
     *
     * <p>The serialized request body is split into individual records to estimate the number of
     * dropped messages, which is emitted as the {@code SINK_MESSAGES_DROP_TOTAL} metric tagged with
     * the response status code, and an informational log line is written.
     *
     * @param response          the HTTP response that triggered the drop
     * @param contentStringList the request body content, one entry per line
     */
    protected void captureMessageDropCount(HttpResponse response, List<String> contentStringList) {
        String requestBody = joptsimple.internal.Strings.join(contentStringList, "\n");

        List<String> result = Arrays.asList(requestBody.replaceAll("^\\[|]$", "").split("},\\s*\\{"));

        getFirehoseInstrumentation().captureCount(Metrics.SINK_MESSAGES_DROP_TOTAL, (long) result.size(), "cause= " + statusCode(response));
        getFirehoseInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }
}
