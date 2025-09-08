package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.exception.ConfigurationException;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;

@Slf4j
public class TimeZoneConverter implements Converter<ZoneId> {
    private static final String DEFAULT_TIMEZONE = "UTC";
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of(DEFAULT_TIMEZONE);

    @Override
    public ZoneId convert(Method method, String input) {
        try {
            if (input == null || input.trim().isEmpty()) {
                log.info("DLQ blob file partition timezone configuration is null or empty, using default timezone: {}", DEFAULT_TIMEZONE);
                return DEFAULT_ZONE_ID;
            }

            String trimmedTimezone = input.trim();
            ZoneId zoneId = ZoneId.of(trimmedTimezone);

            log.info("DLQ blob file partition timezone configuration validated successfully: '{}'", zoneId.getId());

            if (!zoneId.getId().equals(trimmedTimezone)) {
                log.warn("DLQ blob file partition timezone '{}' was normalized to '{}' during validation",
                        trimmedTimezone, zoneId.getId());
            }

            return zoneId;

        } catch (ZoneRulesException e) {
            String errorMessage = String.format(
                    "Invalid DLQ blob file partition timezone configuration '%s'. Please provide a valid timezone identifier. Error: %s",
                    input, e.getMessage());
            log.error(errorMessage);
            throw new ConfigurationException(errorMessage, e);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Unexpected error during DLQ blob file partition timezone configuration validation. Error: %s",
                    e.getMessage());
            log.error(errorMessage);
            throw new ConfigurationException(errorMessage, e);
        }
    }
}
