package com.gotocompany.firehose.sink.common.blobstorage;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.gotocompany.firehose.config.GCSConfig;
import com.gotocompany.firehose.config.ObjectStorageServiceConfig;
import com.gotocompany.firehose.config.S3Config;
import com.gotocompany.firehose.sink.common.blobstorage.cos.CloudObjectStorage;
import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Creates the {@link BlobStorage} implementation for a configured provider.
 *
 * <p>Binds the supplied configuration map to the provider-specific config class and constructs the matching
 * client ({@link com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage},
 * {@link com.gotocompany.firehose.sink.common.blobstorage.s3.S3},
 * {@link com.gotocompany.firehose.sink.common.blobstorage.oss.ObjectStorageService} or
 * {@link com.gotocompany.firehose.sink.common.blobstorage.cos.CloudObjectStorage}).
 */
public class BlobStorageFactory {

    /**
     * Builds the blob storage client for the requested provider.
     *
     * @param storageType the provider to instantiate
     * @param config      the raw configuration bound to the provider-specific config class
     * @return a ready-to-use blob storage client
     * @throws IllegalArgumentException if the provider is unsupported or its client cannot be created
     */
    public static BlobStorage createObjectStorage(BlobStorageType storageType, Map<String, String> config) {
        switch (storageType) {
            case GCS:
                try {
                    GCSConfig gcsConfig = ConfigFactory.create(GCSConfig.class, config);
                    return new GoogleCloudStorage(gcsConfig);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Exception while creating GCS Storage", e);
                }
            case S3:
                try {
                    S3Config s3Config = ConfigFactory.create(S3Config.class, config);
                    return new S3(s3Config);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Exception while creating S3 Storage", e);
                }
            case OSS:
                try {
                    ObjectStorageServiceConfig objectStorageServiceConfig = ConfigFactory.create(ObjectStorageServiceConfig.class, config);
                    return new ObjectStorageService(objectStorageServiceConfig);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Exception while creating OSS Storage", e);
                }
            case COS:
                try {
                    CloudObjectStorageConfig cloudObjectStorageConfig = ConfigFactory.create(CloudObjectStorageConfig.class, config);
                    return new CloudObjectStorage(cloudObjectStorageConfig);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Exception while creating COS Storage", e);
                }
            default:
                throw new IllegalArgumentException("Blob Storage Type " + storageType + " is not supported");
        }
    }
}
