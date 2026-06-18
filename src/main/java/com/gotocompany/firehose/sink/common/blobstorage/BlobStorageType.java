package com.gotocompany.firehose.sink.common.blobstorage;

/**
 * Supported blob/object storage providers.
 *
 * <p>Selects which {@link BlobStorage} implementation {@link BlobStorageFactory} instantiates.
 */
public enum BlobStorageType {
    /** Google Cloud Storage. */
    GCS,
    /** Amazon S3. */
    S3,
    /** Alibaba Cloud Object Storage Service. */
    OSS,
    /** Tencent Cloud Object Storage. */
    COS
}
