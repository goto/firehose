package com.gotocompany.firehose.sink;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.FirehoseMessageUtils;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Firehose sink that adapts a Depot {@link com.gotocompany.depot.Sink} so it can be driven by
 * the Firehose consumer pipeline.
 * <p>
 * Several Firehose sink types (for example log, Redis, BigQuery, BigTable, HTTP v2 and
 * MaxCompute) are implemented in the Depot library. {@code GenericSink} wraps such a Depot sink,
 * extending {@link AbstractSink} to reuse Firehose's instrumentation and error handling. On each
 * batch it buffers the messages in {@link #prepare(List)}, converts them to Depot messages via
 * {@link FirehoseMessageUtils}, pushes them to the Depot sink in {@link #execute()}, and maps any
 * per-record errors from the returned {@link SinkResponse} back onto the original Firehose
 * messages.
 * <p>
 * Instances are created by {@link SinkFactory} for the relevant sink type.
 *
 * @see AbstractSink
 */
public class GenericSink extends AbstractSink {
    /** Buffer holding the current batch staged by {@link #prepare(List)} until {@link #execute()} writes it. */
    private final List<Message> messageList = new ArrayList<>();
    /** The wrapped Depot sink that performs the actual writes. */
    private final Sink sink;

    /**
     * Creates a generic sink that delegates writes to the given Depot sink.
     *
     * @param firehoseInstrumentation the instrumentation used for logging and metrics
     * @param sinkType the sink type name used to tag telemetry
     * @param sink the Depot sink that performs the actual writes
     */
    public GenericSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, Sink sink) {
        super(firehoseInstrumentation, sinkType);
        this.sink = sink;
    }

    /**
     * Converts the buffered batch to Depot messages, pushes them to the wrapped Depot sink, and
     * returns the messages that failed.
     * <p>
     * The Depot {@link SinkResponse} reports failures by batch index; for each failed index the
     * corresponding Firehose {@link Message} is annotated with its error information and included
     * in the returned list.
     *
     * @return the Firehose messages that the Depot sink failed to write
     * @throws Exception if converting or pushing the batch to the Depot sink fails
     */
    @Override
    protected List<Message> execute() throws Exception {
        List<com.gotocompany.depot.message.Message> messages = FirehoseMessageUtils.convertToDepotMessage(messageList);
        SinkResponse response = sink.pushToSink(messages);
        return response.getErrors().keySet().stream()
                .map(index -> {
                    Message message = messageList.get(index.intValue());
                    message.setErrorInfo(response.getErrorsFor(index));
                    return message;
                }).collect(Collectors.toList());
    }

    /**
     * Buffers the incoming batch so it can be written by {@link #execute()}.
     * <p>
     * Replaces the contents of the internal buffer with the supplied messages.
     *
     * @param messages the batch of messages to be written on the next execute
     * @throws DeserializerException if a message cannot be deserialized
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database error occurs
     */
    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        messageList.clear();
        messageList.addAll(messages);
    }

    /**
     * Closes the underlying Depot sink and releases its resources.
     *
     * @throws IOException if closing the Depot sink fails
     */
    @Override
    public void close() throws IOException {
        sink.close();
    }
}
