package com.gotocompany.firehose.sink.jdbc;

import com.gotocompany.firehose.config.JdbcSinkConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.samskivert.mustache.Escapers;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Properties;
import java.util.Enumeration;
import java.util.stream.Collectors;

/**
 * Query template.
 */
public class QueryTemplate {
    /** Mustache skeleton for the {@code INSERT} clause; placeholders are filled on each render. */
    private static final String INSERT_QUERY = "INSERT INTO {{table}} ( {{insertColumns}} ) values ( {{insertValues}} ) ";
    /** Upsert tail appended when unique keys exist and there are non-key columns to update. */
    private static final String UPDATE_CLAUSE = "ON CONFLICT ( {{unique}} ) DO UPDATE SET ( {{updateColumns}} ) = ({{updateValues}})";
    /** Upsert tail appended when unique keys exist but every mapped column is part of the key. */
    private static final String EMPTY_UPDATE_CLAUSE = "ON CONFLICT ( {{unique}} ) DO NOTHING";
    /** Compiled Mustache template produced from the insert (and optional upsert) skeleton. */
    private Template template;
    /** Extracts column values from a message payload using the configured proto-to-column mapping. */
    private ProtoToFieldMapper protoToFieldMapper;
    /** Database columns written on insert, derived from the proto-to-column mapping. */
    private List<String> insertColumns;
    /** Subset of insert columns that are not part of the unique key and are updated on conflict. */
    private List<String> updateColumns;
    /** Unique-key columns parsed from configuration; a non-empty set enables upsert mode. */
    private Set<String> uniqueColumns;
    /** Mutable map of Mustache variables, shared across renders and refreshed per message. */
    private HashMap<String, Object> scopes;
    /** Selects whether the log message ({@code "message"}) or the log key supplies the payload. */
    private String kafkaRecordParserMode;

    /**
     * Instantiates a new Query template.
     *
     * @param jdbcSinkConfig     the jdbc sink config
     * @param protoToFieldMapper the proto to field mapper
     */
    public QueryTemplate(JdbcSinkConfig jdbcSinkConfig, ProtoToFieldMapper protoToFieldMapper) {
        this.protoToFieldMapper = protoToFieldMapper;
        this.insertColumns = new ArrayList<>();
        this.updateColumns = new ArrayList<>();
        this.uniqueColumns = new HashSet<>();
        this.scopes = new HashMap<>();
        this.kafkaRecordParserMode = jdbcSinkConfig.getKafkaRecordParserMode();

        initialize(jdbcSinkConfig);
        buildQuery();
    }

    /**
     * Compiles the Mustache template, appending the upsert clause when unique keys are configured.
     */
    private void buildQuery() {
        String query = isAnUpsertOperation() ? INSERT_QUERY + onConflictResolutionQuery() : INSERT_QUERY;
        template = Mustache.compiler().withEscaper(Escapers.simple()).compile(query);
    }

    /**
     * Returns whether the configuration requested upsert behaviour.
     *
     * @return {@code true} if at least one unique-key column is configured
     */
    private boolean isAnUpsertOperation() {
        return uniqueColumns.size() != 0;
    }

    /**
     * Populates the Mustache scopes and derives the insert, update and unique column sets from config.
     *
     * @param jdbcSinkConfig the configuration supplying the table, unique keys and column mapping
     */
    private void initialize(JdbcSinkConfig jdbcSinkConfig) {
        String uniqueKeys = jdbcSinkConfig.getSinkJdbcUniqueKeys();
        scopes.put("unique", uniqueKeys);
        scopes.put("table", jdbcSinkConfig.getSinkJdbcTableName());

        uniqueColumns = Arrays.stream(uniqueKeys.split(","))
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .collect(Collectors.toSet());

        Properties messageProtoToDBColumnsMapping = jdbcSinkConfig.getInputSchemaProtoToColumnMapping();
        insertColumns = getInsertColumns(messageProtoToDBColumnsMapping);

        updateColumns = selectNonUniqueKeyColumns(uniqueKeys, insertColumns);

        scopes.put("insertColumns", String.join(",", insertColumns));
        scopes.put("updateColumns", String.join(",", updateColumns));
    }

    /**
     * Collects the database column names from the proto-to-column mapping.
     * <p>
     * Nested {@link Properties} values, used for message-typed fields, are flattened recursively so
     * that every leaf column is included.
     *
     * @param messageProtoToDBColumnsMapping the proto-field to column mapping, possibly nested
     * @return the flattened list of column names
     */
    private List<String> getInsertColumns(Properties messageProtoToDBColumnsMapping) {
        List<String> columns = new ArrayList<>();
        Enumeration<?> propertyNames = messageProtoToDBColumnsMapping.propertyNames();
        while (propertyNames.hasMoreElements()) {
            Object tableColumn = messageProtoToDBColumnsMapping.get(propertyNames.nextElement());
            if (tableColumn instanceof String) {
                columns.add((String) tableColumn);
            } else if (tableColumn instanceof Properties) {
                columns.addAll(getInsertColumns((Properties) tableColumn));
            }
        }
        return columns;
    }

    /**
     * Filters out the unique-key columns, leaving the columns eligible for update on conflict.
     *
     * @param uniqueKeys      the configured unique keys as a comma-separated string
     * @param columnsToFilter the full set of insert columns
     * @return the columns that are not part of the unique key
     */
    private List<String> selectNonUniqueKeyColumns(String uniqueKeys, List<String> columnsToFilter) {
        return columnsToFilter.stream().filter(colunmName -> !uniqueKeys.contains(colunmName)).collect(Collectors.toList());
    }

    /**
     * Renders the SQL statement for a single message.
     * <p>
     * Selects the payload bytes based on the configured parser mode ({@code "message"} uses the log
     * message, otherwise the log key), maps them to column values through the proto-to-field mapper,
     * and fills the compiled template with the quoted insert and update values.
     *
     * @param message the message to render into SQL
     * @return the SQL statement string ready for execution
     */
    public String toQueryString(Message message) {

        byte[] value;

        if ("message".equals(kafkaRecordParserMode)) {
            value = message.getLogMessage();
        } else {
            value = message.getLogKey();
        }

        Map<String, Object> columnToValue = protoToFieldMapper.getFields(value);

        String insertValues = stringifyColumnValues(columnToValue, insertColumns);
        String updateValues = stringifyColumnValues(columnToValue, updateColumns);

        scopes.put("updateValues", updateValues);
        scopes.put("insertValues", insertValues);

        return template.execute(scopes);
    }

    /**
     * Builds the comma-separated, single-quoted value list for the given columns.
     * <p>
     * Embedded single quotes are escaped by doubling them so the generated SQL stays valid.
     *
     * @param columnToValue the resolved column-to-value map for the current message
     * @param columns       the ordered columns whose values should be emitted
     * @return the comma-separated list of quoted values
     */
    private String stringifyColumnValues(Map<String, Object> columnToValue, List<String> columns) {
        List<String> columnValues = columns.stream()
                .map(c -> columnToValue.get(c).toString().replace("'", "''"))
                .map(c -> "\'" + c + "\'")
                .collect(Collectors.toList());
        return String.join(", ", columnValues);
    }

    /**
     * Chooses the upsert tail based on whether any non-key columns can be updated.
     *
     * @return {@link #UPDATE_CLAUSE} when there are update columns, otherwise {@link #EMPTY_UPDATE_CLAUSE}
     */
    private String onConflictResolutionQuery() {
        return updateColumns.size() == 0 ? EMPTY_UPDATE_CLAUSE : UPDATE_CLAUSE;
    }
}
