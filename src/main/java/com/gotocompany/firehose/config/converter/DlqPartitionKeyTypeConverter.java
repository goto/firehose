package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class DlqPartitionKeyTypeConverter implements Converter<DlqPartitionKeyType> {
    @Override
    public DlqPartitionKeyType convert(Method method, String input) {
        return DlqPartitionKeyType.valueOf(input.toUpperCase());
    }
}
