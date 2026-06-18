package com.gotocompany.firehose.sink.common;


import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import joptsimple.internal.Strings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base class for HTTP-style sinks that share the request and response handling pipeline.
 *
 * <p>Subclasses turn a batch of {@link com.gotocompany.firehose.message.Message} records into Apache
 * HttpClient requests (typically during {@code prepare}) and register them through {@code setHttpRequests}.
 * This class then owns the common execution loop in {@code execute}: it sends each request, inspects the
 * response status code, logs the request and/or response when configured, and classifies every response as
 * success, retryable or a drop. Retryable messages are returned so the consumer can re-enqueue them, while
 * non-retryable failures are counted as drops through the subclass hook {@code captureMessageDropCount}.
 *
 * <p>Whether a response is retried or merely logged is driven by the two status-code maps supplied at
 * construction time; an absent or unparseable status (for example a transport-level failure) is always
 * treated as retryable. Instances hold mutable per-batch state and are therefore not thread-safe.
 */
public abstract class AbstractHttpSink extends AbstractSink {

    /** Requests prepared for the current batch, sent in order by {@code execute}. */
    private final List<HttpEntityEnclosingRequestBase> httpRequests = new ArrayList<>();
    /** Apache HttpClient used to execute the prepared requests. */
    private final HttpClient httpClient;
    /** Stencil client used to resolve protobuf schemas; closed when the sink is closed. */
    private final StencilClient stencilClient;
    /** Status codes, held as map keys, whose responses must be retried. */
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    /** Status codes, held as map keys, whose requests should be logged. */
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;
    /** Regular expression matching 2xx HTTP status codes that denote success. */
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    /** Source messages for the current batch, aligned by index with {@code getHttpRequests}. */
    private List<Message> sourceMessages;

    /**
     * Initialises the shared HTTP sink state.
     *
     * @param firehoseInstrumentation    instrumentation used to emit metrics and logs
     * @param sinkType                   short sink-type label used in metrics and logs (for example {@code "http"})
     * @param httpClient                 HTTP client used to execute requests
     * @param stencilClient              Stencil client closed when the sink is closed
     * @param retryStatusCodeRanges      map whose keys are the status codes that must be retried
     * @param requestLogStatusCodeRanges map whose keys are the status codes whose request should be logged
     */
    public AbstractHttpSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(firehoseInstrumentation, sinkType);
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    /**
     * Sends every prepared request in order and returns the messages that must be retried.
     *
     * <p>For each request the response status is logged; the response body is logged when debug logging
     * is enabled, and the request is logged when its status code matches the configured log ranges. A
     * response is retried when its status is absent, zero or present in the retry ranges; otherwise a
     * non-2xx status causes the corresponding message to be counted as a drop. The response entity is
     * always consumed and a per-status-code metric is recorded in a {@code finally} block.
     *
     * @return the list of messages whose delivery should be retried
     * @throws Exception if executing a request or reading its content fails
     */
    @Override
    public List<Message> execute() throws Exception {
        HttpResponse response = null;
        ArrayList<Message> failedMessages = new ArrayList<>();
        for (int i = 0; i < httpRequests.size(); i++) {
            try {
                response = httpClient.execute(httpRequests.get(i));
                List<String> contentStringList = null;
                getFirehoseInstrumentation().logInfo("Response Status: {}", statusCode(response));
                if (shouldLogResponse(response)) {
                    printResponse(response);
                }
                if (shouldLogRequest(response)) {
                    contentStringList = readContent(httpRequests.get(i));
                    printRequest(httpRequests.get(i), contentStringList);
                }
                if (shouldRetry(response)) {
                    failedMessages.add(sourceMessages.get(i));
                } else if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
                    contentStringList = contentStringList == null ? readContent(httpRequests.get(i)) : contentStringList;
                    captureMessageDropCount(response, contentStringList);
                }
            } finally {
                consumeResponse(response);
                captureHttpStatusCount(response);
            }
        }
        return failedMessages;
    }

    /**
     * Releases resources held by the sink by clearing pending requests and closing the Stencil client.
     *
     * @throws IOException if closing the Stencil client fails
     */
    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("HTTP connection closing");
        getHttpRequests().clear();
        getStencilClient().close();
    }

    /**
     * Records the batch of source messages so that failed entries can be mapped back after execution.
     *
     * <p>Subclasses override this to additionally build the HTTP requests for the batch, calling
     * {@code super.prepare(messages)} first.
     *
     * @param messages the batch of messages about to be delivered
     * @throws DeserializerException if a message cannot be deserialized
     * @throws IOException           if request preparation performs failing I/O
     * @throws SQLException          never thrown here; declared to satisfy the overridden contract
     */
    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        this.sourceMessages = messages;
    }

    /**
     * Quietly consumes the response entity so the underlying connection can be reused.
     *
     * @param response the response to drain; ignored when {@code null}
     */
    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    /**
     * Determines whether the request should be logged for the given response.
     *
     * @param response the response received for the request
     * @return {@code true} when the status is absent or falls within the configured request-log ranges
     */
    private boolean shouldLogRequest(HttpResponse response) {
        String statusCode = statusCode(response);
        return statusCode.equals("null") || getRequestLogStatusCodeRanges().containsKey(Integer.parseInt(statusCode));
    }

    /**
     * Determines whether the response body should be logged.
     *
     * @param response the response to evaluate
     * @return {@code true} only when debug logging is enabled and the response carries an entity
     */
    private boolean shouldLogResponse(HttpResponse response) {
        return getFirehoseInstrumentation().isDebugEnabled() && response != null && response.getEntity() != null;
    }

    /**
     * Determines whether the message for the given response should be retried.
     *
     * @param response the response received for the request
     * @return {@code true} when the status is absent, zero or present in the configured retry ranges
     */
    private boolean shouldRetry(HttpResponse response) {
        String statusCode = statusCode(response);
        return statusCode.equals("null") || Integer.parseInt(statusCode) == 0 || getRetryStatusCodeRanges().containsKey(Integer.parseInt(statusCode));
    }

    /**
     * Extracts the HTTP status code from a response as a string.
     *
     * @param response the response to inspect, may be {@code null}
     * @return the numeric status code, or the literal {@code "null"} when no status is available
     */
    protected String statusCode(HttpResponse response) {
        if (response != null && response.getStatusLine() != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    /**
     * Emits the per-response status-code counter metric.
     *
     * @param response the response whose status code is recorded; an absent status yields an empty tag value
     */
    private void captureHttpStatusCount(HttpResponse response) {
        String statusCode = statusCode(response);
        String httpCodeTag = statusCode.equals("null") ? "status_code=" : "status_code=" + statusCode;
        getFirehoseInstrumentation().captureCount(Metrics.SINK_HTTP_RESPONSE_CODE_TOTAL, 1L, httpCodeTag);
    }

    /**
     * Logs the method, URI, headers and body of a request at info level.
     *
     * @param httpRequest       the request to log
     * @param contentStringList the request body content, one entry per line
     * @throws IOException if the request details cannot be assembled
     */
    private void printRequest(HttpEntityEnclosingRequestBase httpRequest, List<String> contentStringList) throws IOException {
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                Strings.join(contentStringList, "\n"));
        getFirehoseInstrumentation().logInfo(entireRequest);
    }

    /**
     * Logs the response body at debug level.
     *
     * @param httpResponse the response whose entity is read and logged
     * @throws IOException if the response content cannot be read
     */
    private void printResponse(HttpResponse httpResponse) throws IOException {
        try (InputStream inputStream = httpResponse.getEntity().getContent()) {
            String responseBody = String.format("Response Body: %s",
                    Strings.join(new BufferedReader(new InputStreamReader(
                            inputStream,
                            StandardCharsets.UTF_8)).lines().collect(Collectors.toList()), "\n"));
            getFirehoseInstrumentation().logDebug(responseBody);
        }
    }

    /**
     * Reads the body of the given request, used for logging and drop accounting.
     *
     * @param httpRequest the request whose entity content should be read
     * @return the request body split into lines
     * @throws IOException if the content cannot be read
     */
    protected abstract List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException;

    /**
     * Records the number of messages dropped because of a non-retryable response.
     *
     * @param response      the response that caused the drop
     * @param contentString the request body content, one entry per line
     * @throws IOException if computing the drop count requires failing I/O
     */
    protected abstract void captureMessageDropCount(HttpResponse response, List<String> contentString) throws IOException;

    /**
     * Replaces the set of requests to be sent during the next execution.
     *
     * @param httpRequests the prepared requests for the current batch
     */
    public void setHttpRequests(List<HttpEntityEnclosingRequestBase> httpRequests) {
        this.httpRequests.clear();
        this.httpRequests.addAll(httpRequests);
    }

    /**
     * Returns the mutable list of requests prepared for the current batch.
     *
     * @return the prepared HTTP requests
     */
    public List<HttpEntityEnclosingRequestBase> getHttpRequests() {
        return httpRequests;
    }

    /**
     * Returns the Stencil client used to resolve protobuf schemas.
     *
     * @return the Stencil client
     */
    public StencilClient getStencilClient() {
        return stencilClient;
    }

    /**
     * Returns the status codes that trigger a retry.
     *
     * @return a map whose keys are the retryable status codes
     */
    public Map<Integer, Boolean> getRetryStatusCodeRanges() {
        return retryStatusCodeRanges;
    }

    /**
     * Returns the status codes whose requests should be logged.
     *
     * @return a map whose keys are the loggable status codes
     */
    public Map<Integer, Boolean> getRequestLogStatusCodeRanges() {
        return requestLogStatusCodeRanges;
    }
}
