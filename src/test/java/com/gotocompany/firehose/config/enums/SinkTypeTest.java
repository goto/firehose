package com.gotocompany.firehose.config.enums;

import org.junit.Assert;
import org.junit.Test;

public class SinkTypeTest {

    @Test
    public void shouldIncludeKafkaSinkType() {
        Assert.assertEquals(SinkType.KAFKA, SinkType.valueOf("KAFKA"));
    }
}
