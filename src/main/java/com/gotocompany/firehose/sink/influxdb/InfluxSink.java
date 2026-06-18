package com.gotocompany.firehose.sink.influxdb;



import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.influxdb.builder.PointBuilder;
import com.gotocompany.firehose.sink.AbstractSink;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Influx sink for firehose.
 */
public class InfluxSink extends AbstractSink {
    /** Error message used when no field index mapping is configured; at least one field is required. */
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    /** Sink configuration supplying database name, retention policy and proto mappings. */
    private InfluxSinkConfig config;
    /** Stencil parser used to deserialize each message's log payload into a protobuf message. */
    private Parser protoParser;
    /** Converts each parsed message into an InfluxDB {@link Point}. */
    private PointBuilder pointBuilder;
    /** InfluxDB client used to write the batch. */
    private InfluxDB client;
    /** Points accumulated for the current batch in {@link #prepare(List)}. */
    private BatchPoints batchPoints;
    /** Stencil client used for schema resolution; closed when the sink is closed. */
    private StencilClient stencilClient;

    /**
     * Instantiates a new Influx sink.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param sinkType        the sink type
     * @param config          the config
     * @param protoParser     the proto parser
     * @param client          the client
     * @param stencilClient   the stencil client
     */
    public InfluxSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, InfluxSinkConfig config, Parser protoParser, InfluxDB client, StencilClient stencilClient) {
        super(firehoseInstrumentation, sinkType);
        this.config = config;
        this.protoParser = protoParser;
        this.pointBuilder = new PointBuilder(config);
        this.client = client;
        this.stencilClient = stencilClient;
    }

    /**
     * Builds the batch of InfluxDB points for the messages.
     * <p>
     * Initializes a {@link BatchPoints} for the configured database and retention policy, then parses
     * and converts each message into a {@link Point} that is added to the batch.
     *
     * @param messages the messages to be written in this batch
     * @throws IOException if a message payload cannot be parsed into a point
     */
    @Override
    protected void prepare(List<Message> messages) throws IOException {
        batchPoints = BatchPoints.database(config.getSinkInfluxDbName()).retentionPolicy(config.getSinkInfluxRetentionPolicy()).build();
        for (Message message : messages) {
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());
            Point point = pointBuilder.buildPoint(dynamicMessage);
            getFirehoseInstrumentation().logDebug("Data point: {}", point.toString());
            batchPoints.point(point);
        }
    }

    /**
     * Writes the prepared batch of points to InfluxDB.
     *
     * @return an empty list; the InfluxDB sink does not track per-message failures
     */
    @Override
    protected List<Message> execute() {
        getFirehoseInstrumentation().logDebug("Batch points: {}", batchPoints.toString());
        client.write(batchPoints);
        return new ArrayList<>();
    }

    /**
     * Closes the sink by closing the Stencil client.
     *
     * @throws IOException if closing the Stencil client fails
     */
    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("InfluxDB connection closing");
        stencilClient.close();
    }
}
