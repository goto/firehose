package com.gotocompany.firehose.sink.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps Hikari database connection pool as a JDBCConnectionPool.
 */
public class HikariJdbcConnectionPool implements JdbcConnectionPool {

    /** Minimum connection timeout (ms) HikariCP accepts; smaller values fall back to the driver default. */
    private static final Integer CONNECTION_TIMEOUT_THRESHOLD = 250;
    /** Minimum idle timeout (ms) below which the idle timeout is left at the driver default. */
    private static final Integer IDLE_TIMEOUT_THRESHOLD = 0;
    /** Underlying HikariCP data source that owns the pooled connections. */
    private final HikariDataSource hikariDataSource;

    /**
     * Instantiates a new Hikari jdbc connection pool.
     *
     * @param jdbcUrl           the jdbc url
     * @param username          the username
     * @param password          the password
     * @param maximumPoolSize   the maximum pool size
     * @param connectionTimeout the connection timeout
     * @param idleTimeout       the idle timeout
     * @param minimumIdle       the minimum idle
     */
    public HikariJdbcConnectionPool(String jdbcUrl, String username, String password, int maximumPoolSize,
                                    long connectionTimeout, long idleTimeout, int minimumIdle) {
        HikariConfig config = new HikariConfig();
        config.setRegisterMbeans(true);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        if (connectionTimeout >= CONNECTION_TIMEOUT_THRESHOLD) {
            config.setConnectionTimeout(connectionTimeout);
        }
        if (idleTimeout >= IDLE_TIMEOUT_THRESHOLD) {
            config.setIdleTimeout(idleTimeout);
        }
        hikariDataSource = new HikariDataSource(config);
    }

    /**
     * Creates a pool wrapping an already-configured Hikari data source, primarily for testing.
     *
     * @param hikariDataSource the data source to wrap
     */
    HikariJdbcConnectionPool(HikariDataSource hikariDataSource) {
        this.hikariDataSource = hikariDataSource;
    }

    /**
     * Borrows a connection from the underlying Hikari data source.
     *
     * @return a pooled JDBC connection
     * @throws SQLException if a connection cannot be obtained from the pool
     */
    @Override
    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    /**
     * Returns a connection to the pool by closing it; HikariCP recycles it for reuse.
     *
     * @param connection the connection to release
     * @throws SQLException if closing the connection fails
     */
    @Override
    public void release(Connection connection) throws SQLException {
        connection.close();
    }

    /**
     * Closes the underlying Hikari data source and all of its connections.
     */
    @Override
    public void shutdown() {
        hikariDataSource.close();
    }
}
