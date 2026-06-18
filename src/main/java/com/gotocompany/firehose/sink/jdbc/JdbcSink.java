package com.gotocompany.firehose.sink.jdbc;


import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC Sink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for JDBC Sink can be found here: {@see com.gotocompany.firehose.config.JdbcSinkConfig}
 */
public class JdbcSink extends AbstractSink {

    /** Pool that supplies and reclaims the JDBC connections shared across batches. */
    private JdbcConnectionPool pool;
    /** Template that renders a {@link Message} into an SQL insert or upsert statement. */
    private QueryTemplate queryTemplate;
    /** Stencil client used for protobuf schema resolution; closed when the sink is closed. */
    private StencilClient stencilClient;
    /** JDBC statement accumulating the current batch; created in {@link #prepare(List)} and run in {@link #execute()}. */
    private Statement statement;
    /** Connection backing the current batch; borrowed in {@link #prepare(List)} and released in {@link #execute()}. */
    private Connection connection = null;

    /**
     * Instantiates a new Jdbc sink.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param sinkType        the sink type
     * @param pool            the pool
     * @param queryTemplate   the query template
     * @param stencilClient   the stencil client
     */
    public JdbcSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient) {
        super(firehoseInstrumentation, sinkType);
        this.pool = pool;
        this.queryTemplate = queryTemplate;
        this.stencilClient = stencilClient;
    }

    /**
     * Creates a JDBC sink with pre-supplied {@link Statement} and {@link Connection} instances.
     * <p>
     * This package-private constructor exists mainly to inject test doubles for the JDBC resources.
     *
     * @param firehoseInstrumentation the instrumentation used for logging and metric emission
     * @param sinkType                a short label identifying the sink type in metrics
     * @param pool                    the connection pool from which connections are borrowed and released
     * @param queryTemplate           the template that renders each message into an SQL statement
     * @param stencilClient           the Stencil client used for protobuf schema resolution
     * @param statement               the pre-created JDBC statement used to accumulate the batch
     * @param connection              the pre-created JDBC connection backing the statement
     */
    JdbcSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient, Statement statement, Connection connection) {
        this(firehoseInstrumentation, sinkType, pool, queryTemplate, stencilClient);
        this.statement = statement;
        this.connection = connection;
    }

    /**
     * Prepares the batch by rendering each message into SQL and queuing it onto a JDBC batch.
     * <p>
     * Borrows a {@link Connection} from the pool, creates a {@link Statement}, and adds one query per
     * message. The connection and statement are retained for the subsequent {@link #execute()} call.
     *
     * @param messages the messages to be persisted in this batch
     * @throws SQLException if obtaining the connection, creating the statement, or adding a query fails
     */
    @Override
    protected void prepare(List<Message> messages) throws SQLException {
        List<String> queriesList = createQueries(messages);
        connection = pool.getConnection();
        statement = connection.createStatement();

        for (String query : queriesList) {
            statement.addBatch(query);
        }
    }

    /**
     * Renders the given messages into SQL statements using the configured {@link QueryTemplate}.
     * <p>
     * Each rendered query is also logged at debug level.
     *
     * @param messages the messages to convert
     * @return the SQL statements, one per message, in input order
     */
    protected List<String> createQueries(List<Message> messages) {
        List<String> queries = new ArrayList<>();
        for (Message message : messages) {
            String queryString = queryTemplate.toQueryString(message);
            getFirehoseInstrumentation().logDebug(queryString);
            queries.add(queryString);
        }
        return queries;
    }

    /**
     * Executes the previously prepared JDBC batch against the database.
     * <p>
     * The per-row update counts returned by the driver are logged at debug level, and the borrowed
     * connection is always released back to the pool, even when execution fails.
     *
     * @return an empty list; the JDBC sink surfaces a failed batch as a thrown exception handled by
     *     {@link AbstractSink}
     * @throws Exception if executing the batch fails
     */
    @Override
    protected List<Message> execute() throws Exception {
        try {
            int[] updateCounts = statement.executeBatch();
            getFirehoseInstrumentation().logDebug("DB response: {}", Arrays.toString(updateCounts));
        } finally {
            if (connection != null) {
                pool.release(connection);
            }
        }
        return new ArrayList<>();
    }

    /**
     * Closes the sink, shutting down the connection pool and the Stencil client.
     *
     * @throws IOException if the pool shutdown is interrupted; the {@link InterruptedException} is wrapped
     */
    @Override
    public void close() throws IOException {
        try {
            getFirehoseInstrumentation().logInfo("Database connection closing");
            pool.shutdown();
            stencilClient.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
