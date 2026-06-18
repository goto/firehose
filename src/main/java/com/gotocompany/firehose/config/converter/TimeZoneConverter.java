package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.exception.ConfigurationException;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;

/**
 * Owner {@link Converter} that resolves a timezone configuration string into a
 * {@link java.time.ZoneId}, used when partitioning DLQ blob files by time.
 *
 * <p>A {@code null}, empty or whitespace-only value falls back to the default {@code UTC} zone.
 * Otherwise the trimmed value is parsed with {@link java.time.ZoneId#of(String)}, and a value that
 * is normalized to a different identifier is logged as a warning. Any parsing failure is wrapped in
 * a {@link com.gotocompany.firehose.exception.ConfigurationException}.
 */
@Slf4j
public class TimeZoneConverter implements Converter<ZoneId> {
    /**
     * Fallback timezone identifier used when no timezone is configured.
     */
    private static final String DEFAULT_TIMEZONE = "UTC";
    /**
     * Pre-resolved {@link java.time.ZoneId} for the default {@code UTC} timezone.
     */
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of(DEFAULT_TIMEZONE);

    /**
     * Converts the configured timezone value into a {@link java.time.ZoneId}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw timezone identifier; a {@code null}, empty or blank value selects
     *     {@code UTC}
     * @return the resolved {@link java.time.ZoneId}, or the default {@code UTC} zone when no value is
     *     supplied
     * @throws com.gotocompany.firehose.exception.ConfigurationException if the value is not a valid
     *     timezone identifier or cannot otherwise be parsed
     */
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
