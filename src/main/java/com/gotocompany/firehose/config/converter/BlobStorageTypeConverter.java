package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a blob-storage provider configuration string into a
 * {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType} constant.
 *
 * <p>This selects the object-store backend (for example Google Cloud Storage, Amazon S3, Alibaba
 * OSS or Tencent COS) used by the blob sink and the blob-based DLQ writer. The input is upper-cased
 * before lookup; an unrecognised value propagates the {@code IllegalArgumentException} thrown by the
 * enum lookup.
 */
public class BlobStorageTypeConverter implements Converter<BlobStorageType> {
    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value naming the storage provider (any case)
     * @return the matching {@link com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType} constant
     * @throws IllegalArgumentException if the value does not name a valid constant
     */
    @Override
    public BlobStorageType convert(Method method, String input) {
        return BlobStorageType.valueOf(input.toUpperCase());
    }
}
