package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.dlq.DlqPartitionKeyType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DlqPartitionKeyTypeConverterTest {

    private DlqPartitionKeyTypeConverter converter;

    @Before
    public void setUp() {
        converter = new DlqPartitionKeyTypeConverter();
    }

    @Test
    public void shouldReturnProduceTimestampForLowercaseInput() {
        DlqPartitionKeyType result = converter.convert(null, "produce_timestamp");
        assertEquals(DlqPartitionKeyType.PRODUCE_TIMESTAMP, result);
    }

    @Test
    public void shouldReturnProduceTimestampForUppercaseInput() {
        DlqPartitionKeyType result = converter.convert(null, "PRODUCE_TIMESTAMP");
        assertEquals(DlqPartitionKeyType.PRODUCE_TIMESTAMP, result);
    }

    @Test
    public void shouldReturnConsumeTimestampForMixedCaseInput() {
        DlqPartitionKeyType result = converter.convert(null, "CoNsUmE_TiMeStAmP");
        assertEquals(DlqPartitionKeyType.CONSUME_TIMESTAMP, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidInput() {
        converter.convert(null, "invalid");
    }
}
