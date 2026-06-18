package com.gotocompany.firehose.sink.jdbc.field;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.List;

/**
 * Jdbc map field.
 */
public class JdbcMapField implements JdbcField {
    /** The raw map field value, expected to be a list of protobuf entry messages. */
    private Object columnValue;
    /** Descriptor of the field, used to confirm it is a protobuf map field. */
    private Descriptors.FieldDescriptor fieldDescriptor;


    /**
     * Creates a map-field converter.
     *
     * @param columnValue     the raw map field value (a list of entry messages)
     * @param fieldDescriptor the descriptor of the map field
     */
    public JdbcMapField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        this.columnValue = columnValue;
        this.fieldDescriptor = fieldDescriptor;
    }

    /**
     * Serializes the protobuf map entries into a JSON object string.
     * <p>
     * Each entry's first field is used as the key and the second, when present, as the value.
     *
     * @return the JSON object string representing the map
     * @throws RuntimeException if the value is not the expected list of entry messages
     */
    @Override
    public Object getColumn() throws RuntimeException {
        HashMap<String, Object> columnFields = new HashMap<>();
        List<DynamicMessage> values = (List<DynamicMessage>) this.columnValue;
        for (DynamicMessage dynamicMessage : values) {
            Object[] data = dynamicMessage.getAllFields().values().toArray();
            Object mapValue = data.length > 1 ? data[1] : "";
            columnFields.put((String) data[0], mapValue);
        }
        String columnEntry = JSONObject.toJSONString(columnFields);
        return columnEntry;
    }

    /**
     * Indicates whether the field is a protobuf map field.
     *
     * @return {@code true} if the field descriptor represents a map field
     */
    @Override
    public boolean canProcess() {
        return fieldDescriptor.isMapField();
    }
}
