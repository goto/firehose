package com.gotocompany.firehose.sink.elasticsearch;

import com.gotocompany.firehose.exception.NeedToRetry;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.elasticsearch.request.EsRequestHandler;
import com.gotocompany.firehose.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Elastic search sink.
 */
public class EsSink extends AbstractSink {
    /** Elasticsearch REST client used to send bulk requests; closed on {@link #close()}. */
    private RestHighLevelClient client;
    /** Builds an Elasticsearch write request (index or update) for each message. */
    private EsRequestHandler esRequestHandler;
    /** The bulk request assembled for the current batch in {@link #prepare(List)}. */
    private BulkRequest bulkRequest;
    /** Per-request timeout in milliseconds applied to the bulk request. */
    private long esRequestTimeoutInMs;
    /** Number of active shards that must acknowledge each write before it succeeds. */
    private Integer esWaitForActiveShardsCount;
    /** Response status codes that must not be retried; matching failures are dropped instead. */
    private List<String> esRetryStatusCodeBlacklist;

    /**
     * Instantiates a new Es sink.
     *
     * @param firehoseInstrumentation            the instrumentation
     * @param sinkType                   the sink type
     * @param client                     the client
     * @param esRequestHandler           the es request handler
     * @param esRequestTimeoutInMs       the es request timeout in ms
     * @param esWaitForActiveShardsCount the es wait for active shards count
     * @param esRetryStatusCodeBlacklist the es retry status code blacklist
     */
    public EsSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, RestHighLevelClient client, EsRequestHandler esRequestHandler,
                  long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount, List<String> esRetryStatusCodeBlacklist) {
        super(firehoseInstrumentation, sinkType);
        this.client = client;
        this.esRequestHandler = esRequestHandler;
        this.esRequestTimeoutInMs = esRequestTimeoutInMs;
        this.esWaitForActiveShardsCount = esWaitForActiveShardsCount;
        this.esRetryStatusCodeBlacklist = esRetryStatusCodeBlacklist;
    }

    /**
     * Builds the bulk request for the batch.
     * <p>
     * Applies the configured request timeout and wait-for-active-shards count, then adds one write
     * request per message produced by the {@link EsRequestHandler}.
     *
     * @param messages the messages to be written in this batch
     */
    @Override
    protected void prepare(List<Message> messages) {
        bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMillis(esRequestTimeoutInMs));
        bulkRequest.waitForActiveShards(esWaitForActiveShardsCount);
        messages.forEach(message -> bulkRequest.add(esRequestHandler.getRequest(message)));
    }

    /**
     * Sends the bulk request and handles any failures.
     * <p>
     * On failures, non-blacklisted errors raise a {@link NeedToRetry} so the batch is retried, while
     * blacklisted errors are logged, dropped and counted.
     *
     * @return an empty list; failures are surfaced either as drops or via a thrown exception
     * @throws Exception if the bulk request fails or a non-blacklisted item error is encountered
     */
    @Override
    protected List<Message> execute() throws Exception {
        BulkResponse bulkResponse = getBulkResponse();
        if (bulkResponse.hasFailures()) {
            getFirehoseInstrumentation().logWarn("Bulk request failed");
            handleResponse(bulkResponse);
        }
        return new ArrayList<>();
    }

    /**
     * Closes the sink by closing the Elasticsearch REST client.
     *
     * @throws IOException if closing the client fails
     */
    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("Elastic Search connection closing");
        this.client.close();
    }

    /**
     * Executes the prepared bulk request against Elasticsearch.
     *
     * @return the bulk response returned by the cluster
     * @throws IOException if the request cannot be sent or the response cannot be read
     */
    BulkResponse getBulkResponse() throws IOException {
        return client.bulk(bulkRequest);
    }

    /**
     * Inspects the bulk response and decides whether to drop or retry each failed item.
     * <p>
     * A failed item whose status code is in the retry blacklist is logged, dropped and counted; any
     * other failure raises {@link NeedToRetry} to retry the whole batch.
     *
     * @param bulkResponse the response returned by the bulk request
     * @throws NeedToRetry if any failed item has a non-blacklisted status code
     */
    private void handleResponse(BulkResponse bulkResponse) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponseCount++;
                String responseStatus = String.valueOf(response.status().getStatus());
                if (esRetryStatusCodeBlacklist.contains(responseStatus)) {
                    getFirehoseInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                    getFirehoseInstrumentation().incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=" + response.status().name());
                    getFirehoseInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
                } else {
                    throw new NeedToRetry(bulkResponse.buildFailureMessage());
                }
            }
        }
        getFirehoseInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
