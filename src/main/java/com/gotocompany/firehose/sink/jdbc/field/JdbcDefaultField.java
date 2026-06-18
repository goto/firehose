package com.gotocompany.firehose.sink.jdbc.field;

/**
 * Fallback {@link JdbcField} that returns the raw value unchanged.
 * <p>
 * Used by {@link JdbcFieldFactory} when no specialized strategy matches; {@link #canProcess()} always
 * returns {@code false}, so it is only ever selected as the explicit default.
 */
public class JdbcDefaultField implements JdbcField {
    /** The raw field value returned as-is from {@link #getColumn()}. */
    private Object columnValue;

    /**
     * Creates a default field wrapping the given value.
     *
     * @param columnValue the value to return unchanged
     */
    public JdbcDefaultField(Object columnValue) {
        this.columnValue = columnValue;
    }

    /**
     * Returns the wrapped value unchanged.
     *
     * @return the original column value
     * @throws RuntimeException never thrown by this implementation; declared for interface compatibility
     */
    @Override
    public Object getColumn() throws RuntimeException {
        return columnValue;
    }

    /**
     * Always returns {@code false} so this fallback is never auto-selected.
     *
     * @return {@code false}
     */
    @Override
    public boolean canProcess() {
        return false;
    }
}
