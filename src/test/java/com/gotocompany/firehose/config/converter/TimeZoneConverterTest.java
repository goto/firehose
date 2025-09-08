package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.exception.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.time.ZoneId;

import static org.junit.Assert.*;

public class TimeZoneConverterTest {

    private TimeZoneConverter converter;
    private Method method;

    @Before
    public void setUp() throws Exception {
        converter = new TimeZoneConverter();
        method = TimeZoneConverterTest.class.getMethod("dummyMethod");
    }

    @Test
    public void shouldConvertValidTimezone() {
        ZoneId result = converter.convert(method, "Asia/Tokyo");
        assertEquals(ZoneId.of("Asia/Tokyo"), result);
    }

    @Test
    public void shouldConvertUTCTimezone() {
        ZoneId result = converter.convert(method, "UTC");
        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldConvertOffsetTimezone() {
        ZoneId result = converter.convert(method, "+05:30");
        assertEquals(ZoneId.of("+05:30"), result);
    }

    @Test
    public void shouldReturnUTCForNullInput() {
        ZoneId result = converter.convert(method, null);
        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldReturnUTCForEmptyInput() {
        ZoneId result = converter.convert(method, "");
        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldReturnUTCForWhitespaceInput() {
        ZoneId result = converter.convert(method, "   ");
        assertEquals(ZoneId.of("UTC"), result);
    }

    @Test
    public void shouldTrimWhitespaceAroundValidTimezone() {
        ZoneId result = converter.convert(method, "  Asia/Jakarta  ");
        assertEquals(ZoneId.of("Asia/Jakarta"), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionForInvalidTimezone() {
        converter.convert(method, "Invalid/Timezone");
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionForMalformedTimezone() {
        converter.convert(method, "NotAValidTimezone");
    }

    @Test
    public void shouldConvertGMTTimezone() {
        ZoneId result = converter.convert(method, "GMT");
        assertEquals(ZoneId.of("GMT"), result);
    }

    @Test
    public void shouldConvertZuluTimezone() {
        ZoneId result = converter.convert(method, "Z");
        assertEquals(ZoneId.of("Z"), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldRejectUnsupportedLegacyTimezoneIds() {
        converter.convert(method, "EST");
    }

    @Test
    public void shouldConvertNegativeOffsetTimezone() {
        ZoneId result = converter.convert(method, "-08:00");
        assertEquals(ZoneId.of("-08:00"), result);
    }

    @Test
    public void shouldConvertUTCPlusOffsetTimezone() {
        ZoneId result = converter.convert(method, "UTC+09:00");
        assertEquals(ZoneId.of("UTC+09:00"), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionForInvalidOffsetFormat() {
        converter.convert(method, "+25:00");
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionForInvalidHourFormat() {
        converter.convert(method, "+AB:00");
    }

    public void dummyMethod() {
    }
}
