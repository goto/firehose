package com.gotocompany.firehose.config;

/**
 * Owner configuration for the JDBC sink, which writes consumed messages into a relational database.
 *
 * <p>It supplies the JDBC connection coordinates (URL, credentials and target table), the unique
 * keys used for upserts, and the HikariCP connection-pool tunables. Each accessor maps to an
 * environment variable via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface JdbcSinkConfig extends AppConfig {

    /**
     * Returns the JDBC connection URL of the target database, set by {@code SINK_JDBC_URL}.
     *
     * @return the JDBC URL
     */
    @Key("SINK_JDBC_URL")
    String getSinkJdbcUrl();

    /**
     * Returns the username used to authenticate to the database, set by {@code SINK_JDBC_USERNAME}.
     *
     * @return the JDBC username
     */
    @Key("SINK_JDBC_USERNAME")
    String getSinkJdbcUsername();

    /**
     * Returns the password used to authenticate to the database, set by {@code SINK_JDBC_PASSWORD}.
     *
     * @return the JDBC password
     */
    @Key("SINK_JDBC_PASSWORD")
    String getSinkJdbcPassword();

    /**
     * Returns the name of the table messages are written to, set by {@code SINK_JDBC_TABLE_NAME}.
     *
     * @return the target table name
     */
    @Key("SINK_JDBC_TABLE_NAME")
    String getSinkJdbcTableName();

    /**
     * Returns the comma-separated column names that form the unique key used for upserts, set by
     * {@code SINK_JDBC_UNIQUE_KEYS} and defaulting to an empty string (plain inserts).
     *
     * @return the unique key columns
     */
    @Key("SINK_JDBC_UNIQUE_KEYS")
    @DefaultValue("")
    String getSinkJdbcUniqueKeys();

    /**
     * Returns the maximum number of connections in the HikariCP pool, set by
     * {@code SINK_JDBC_CONNECTION_POOL_MAX_SIZE}.
     *
     * @return the maximum connection pool size
     */
    @Key("SINK_JDBC_CONNECTION_POOL_MAX_SIZE")
    Integer getSinkJdbcConnectionPoolMaxSize();

    /**
     * Returns the maximum time in milliseconds to wait for a connection from the pool, set by
     * {@code SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS}.
     *
     * @return the connection acquisition timeout in milliseconds
     */
    @Key("SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS")
    Integer getSinkJdbcConnectionPoolTimeoutMs();

    /**
     * Returns the time in milliseconds an idle connection may stay in the pool before being
     * retired, set by {@code SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS}.
     *
     * @return the idle connection timeout in milliseconds
     */
    @Key("SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS")
    Integer getSinkJdbcConnectionPoolIdleTimeoutMs();

    /**
     * Returns the minimum number of idle connections HikariCP keeps in the pool, set by
     * {@code SINK_JDBC_CONNECTION_POOL_MIN_IDLE}.
     *
     * @return the minimum idle connection count
     */
    @Key("SINK_JDBC_CONNECTION_POOL_MIN_IDLE")
    Integer getSinkJdbcConnectionPoolMinIdle();
}
