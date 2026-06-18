package com.gotocompany.firehose.sink.jdbc.field.message;

import com.gotocompany.firehose.sink.jdbc.field.JdbcField;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link JdbcField} that serializes a repeated (collection) protobuf field into a JSON array string.
 * <p>
 * When the collection holds protobuf {@link Message} elements, each element is converted to JSON via
 * {@link JdbcDefaultMessageField} and joined into a JSON array; otherwise the whole collection is
 * serialized with Gson. Applies to repeated fields that are not protobuf maps.
 */
public class JdbcCollectionField implements JdbcField {
    /** The raw repeated field value, expected to be a {@link Collection}. */
    private Object columnValue;
    /** Descriptor of the field, used to exclude protobuf map fields. */
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Creates a collection-field converter.
     *
     * @param columnValue     the raw repeated field value
     * @param fieldDescriptor the descriptor of the repeated field
     */
    public JdbcCollectionField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        this.columnValue = columnValue;
        this.fieldDescriptor = fieldDescriptor;
    }

    /**
     * Serializes the collection into a JSON array string.
     * <p>
     * If the first element is a protobuf {@link Message}, every element is converted with
     * {@link JdbcDefaultMessageField} and wrapped in a JSON array; otherwise the collection is
     * serialized directly with Gson.
     *
     * @return the JSON array string representing the collection
     * @throws RuntimeException if the value cannot be processed as a collection
     */
    @Override
    public Object getColumn() throws RuntimeException {
        Collection collectionOfMessages = (Collection) columnValue;
        Optional first = collectionOfMessages.stream().findFirst();
        if (first.isPresent() && first.get() instanceof Message) {
            Object messageJsons = collectionOfMessages
                    .stream()
                    .map(cValue -> new JdbcDefaultMessageField(cValue).getColumn().toString())
                    .collect(Collectors.joining(","));
            return "[" + messageJsons + "]";
        } else {
            return new GsonBuilder().create().toJson(collectionOfMessages);
        }
    }

    /**
     * Indicates whether the value is a non-map collection.
     *
     * @return {@code true} if the value is a {@link Collection} and the field is not a map field
     */
    @Override
    public boolean canProcess() {
        return columnValue instanceof Collection && !fieldDescriptor.isMapField();
    }
}
