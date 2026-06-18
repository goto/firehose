package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.blob.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the blob sink file-partitioning configuration string into a
 * {@code Constants.FilePartitionType} (declared in
 * {@link com.gotocompany.firehose.sink.blob.Constants}).
 *
 * <p>The partition type decides how the blob sink groups buffered records into output file paths
 * (for example by date or by hour). The input is upper-cased before lookup; an unrecognised value
 * propagates the {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class BlobSinkFilePartitionTypeConverter implements Converter<Constants.FilePartitionType> {
    /**
     * Converts the raw configuration value into a {@code Constants.FilePartitionType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the file-partition strategy (any case)
     * @return the matching {@code Constants.FilePartitionType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public Constants.FilePartitionType convert(Method method, String input) {
        return Constants.FilePartitionType.valueOf(input.toUpperCase());
    }
}
