package com.gotocompany.firehose.config;

import com.gotocompany.firehose.exception.ConfigurationException;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DlqConfigTest {

    @Test
    public void shouldLoadValidTimezoneFromConfiguration() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "Asia/Tokyo");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("Asia/Tokyo"), result);
    }

    @Test
    public void shouldUseDefaultTimezoneWhenConfigNotProvided() {
        Map<String, String> configMap = new HashMap<>();

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldLoadSystemTimezone() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "America/Chicago");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("America/Chicago"), result);
    }

    @Test
    public void shouldHandleEmptyConfigurationValue() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldHandleWhitespaceConfigurationValue() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "   ");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldTrimWhitespaceAroundValidTimezone() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "  Europe/London  ");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("Europe/London"), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailForInvalidTimezoneAtConfigLoad() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "Invalid/Timezone");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        config.getDlqBlobFilePartitionTimezone();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailForMalformedTimezoneAtConfigLoad() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "NotAValidTimezone");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        config.getDlqBlobFilePartitionTimezone();
    }

    @Test
    public void shouldLoadOffsetBasedTimezone() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "+05:30");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        ZoneId result = config.getDlqBlobFilePartitionTimezone();

        assertEquals(ZoneId.of("+05:30"), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldRejectUnsupportedLegacyTimezoneIds() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("DLQ_BLOB_FILE_PARTITION_TIMEZONE", "EST");

        DlqConfig config = ConfigFactory.create(DlqConfig.class, configMap);
        config.getDlqBlobFilePartitionTimezone();
    }
}
