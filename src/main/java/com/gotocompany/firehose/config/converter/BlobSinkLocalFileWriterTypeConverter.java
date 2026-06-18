package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.blob.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves the blob sink local file-writer configuration string into a
 * {@code Constants.WriterType} (declared in {@link com.gotocompany.firehose.sink.blob.Constants}).
 *
 * <p>The writer type selects the on-disk encoding the blob sink uses while staging records locally
 * before upload (for example Parquet). The input is upper-cased before lookup; an unrecognised value
 * propagates the {@code IllegalArgumentException} thrown by the enum lookup.
 */
public class BlobSinkLocalFileWriterTypeConverter implements Converter<Constants.WriterType> {
    /**
     * Converts the raw configuration value into a {@code Constants.WriterType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the local file-writer type (any case)
     * @return the matching {@code Constants.WriterType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public Constants.WriterType convert(Method method, String input) {
        return Constants.WriterType.valueOf(input.toUpperCase());
    }
}
