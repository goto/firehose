package com.gotocompany.firehose.converter;

import com.google.protobuf.Timestamp;

import java.util.concurrent.TimeUnit;

/**
 * Converts protobuf timestamps into epoch time expressed in nanoseconds.
 *
 * <p>Helper used when a sink needs a single nanosecond value from a protobuf
 * {@code google.protobuf.Timestamp} (seconds plus a nanosecond offset).
 */
public class ProtoTimeConverter {

    /**
     * Combines a seconds value and a nanosecond offset into total epoch nanoseconds.
     *
     * @param timeInSeconds the epoch time in seconds
     * @param nanosOffset   the additional nanoseconds within the second
     * @return the epoch time in nanoseconds
     */
    public static long getEpochTimeInNanoSeconds(long timeInSeconds, int nanosOffset) {
        return TimeUnit.NANOSECONDS.convert(timeInSeconds, TimeUnit.SECONDS) + nanosOffset;
    }

    /**
     * Converts a protobuf {@link Timestamp} into epoch nanoseconds.
     *
     * @param time the protobuf timestamp
     * @return the epoch time in nanoseconds
     */
    public static long getEpochTimeInNanoSeconds(Timestamp time) {
        return getEpochTimeInNanoSeconds(time.getSeconds(), time.getNanos());
    }
}
