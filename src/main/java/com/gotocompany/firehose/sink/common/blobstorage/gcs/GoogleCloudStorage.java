package com.gotocompany.firehose.sink.common.blobstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.gotocompany.firehose.config.GCSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.gcs.error.GCSErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * {@link BlobStorage} implementation backed by Google Cloud Storage.
 *
 * <p>Wraps a GCS {@link Storage} client configured from {@link GCSConfig} (project, credentials and retry
 * settings). On construction it verifies the target bucket exists and logs its retention policy. Objects are
 * written under an optional directory prefix; upload failures are mapped to a {@link BlobStorageException}
 * whose error type comes from {@link GCSErrorType}.
 */
public class GoogleCloudStorage implements BlobStorage {
    /** Logger for bucket checks, retention policy and upload outcomes. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudStorage.class);
    /** Bound GCS configuration (project, bucket, prefix and retry settings). */
    private final GCSConfig gcsConfig;
    /** GCS client used to perform the uploads. */
    private final Storage storage;

    /**
     * Creates a GCS storage client from configuration, verifying the bucket and logging its retention policy.
     *
     * <p>Loads service-account credentials from the path in the configuration.
     *
     * @param gcsConfig the bound GCS configuration
     * @throws IOException              if the credentials file cannot be read
     * @throws IllegalArgumentException if the configured bucket does not exist
     */
    public GoogleCloudStorage(GCSConfig gcsConfig) throws IOException {
        this(gcsConfig, GoogleCredentials.fromStream(Files.newInputStream(Paths.get(gcsConfig.getGCSCredentialPath()))));
        checkBucket();
        logRetentionPolicy();
    }

    /**
     * Creates a GCS storage client from configuration and explicit credentials.
     *
     * <p>Builds a {@code Storage} service with the configured project id and retry settings.
     *
     * @param gcsConfig   the bound GCS configuration
     * @param credentials the Google credentials used to authenticate
     */
    public GoogleCloudStorage(GCSConfig gcsConfig, GoogleCredentials credentials) {
        this(gcsConfig, StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getGCloudProjectID())
                .setCredentials(credentials)
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(gcsConfig.getGCSRetryMaxAttempts())
                        .setInitialRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryInitialDelayMS()))
                        .setMaxRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryMaxDelayMS()))
                        .setRetryDelayMultiplier(gcsConfig.getGCSRetryDelayMultiplier())
                        .setTotalTimeout(Duration.ofMillis(gcsConfig.getGCSRetryTotalTimeoutMS()))
                        .setInitialRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryInitialRPCTimeoutMS()))
                        .setRpcTimeoutMultiplier(gcsConfig.getGCSRetryRPCTimeoutMultiplier())
                        .setMaxRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryRPCMaxTimeoutMS()))
                        .build())
                .build().getService());
    }

    /**
     * Creates a GCS storage client using a caller-supplied {@code Storage}, primarily for testing.
     *
     * @param gcsConfig the bound GCS configuration
     * @param storage   the GCS client to use
     */
    public GoogleCloudStorage(GCSConfig gcsConfig, Storage storage) {
        this.gcsConfig = gcsConfig;
        this.storage = storage;
    }

    /**
     * Verifies that the configured bucket exists.
     *
     * @throws IllegalArgumentException if the bucket does not exist
     */
    private void checkBucket() {
        String bucketName = gcsConfig.getGCSBucketName();
        Bucket bucket = storage.get(bucketName, Storage.BucketGetOption.userProject(gcsConfig.getGCloudProjectID()));
        if (bucket == null) {
            LOGGER.info("Bucket does not exist:{}", bucketName);
            LOGGER.info("Please create GCS bucket before running firehose: " + bucketName);
            throw new IllegalArgumentException("GCS Bucket not found " + bucketName);
        }
    }

    /**
     * Logs the retention policy and period configured on the bucket.
     */
    private void logRetentionPolicy() {
        String bucketName = gcsConfig.getGCSBucketName();
        Bucket bucket = storage.get(
                bucketName,
                Storage.BucketGetOption.fields(Storage.BucketField.RETENTION_POLICY),
                Storage.BucketGetOption.userProject(gcsConfig.getGCloudProjectID()));
        LOGGER.info("Retention Policy for {}", bucketName);
        LOGGER.info("Retention Period: {}", bucket.getRetentionPeriodDuration());
        if (bucket.retentionPolicyIsLocked() != null && bucket.retentionPolicyIsLocked()) {
            LOGGER.info("Retention Policy is locked");
        }
    }

    /**
     * Reads a local file and uploads its contents under the given object name.
     *
     * @param objectName the destination object name, before applying the directory prefix
     * @param filePath   the path of the local file to upload
     * @throws BlobStorageException if the file cannot be read or the upload fails
     */
    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        String finalPath = createPath(objectName);
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(finalPath, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }
    }

    /**
     * Prepends the configured directory prefix to the object name, when set.
     *
     * @param objectName the base object name
     * @return the full object name including any directory prefix
     */
    private String createPath(String objectName) {
        String prefix = gcsConfig.getGCSDirectoryPrefix();
        return prefix == null || prefix.isEmpty()
                ? objectName : Paths.get(prefix, objectName).toString();
    }

    /**
     * Uploads the given bytes under the given object name, applying the directory prefix.
     *
     * @param objectName the destination object name, before applying the directory prefix
     * @param content    the bytes to upload
     * @throws BlobStorageException if the upload fails; its error type reflects the GCS status code
     */
    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        String finalPath = createPath(objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(gcsConfig.getGCSBucketName(), finalPath)).build();
        String blobPath = String.join(File.separator, blobInfo.getBucket(), blobInfo.getName());
        try {
            storage.create(blobInfo, content, Storage.BlobTargetOption.userProject(gcsConfig.getGCloudProjectID()));
            LOGGER.info("Created object in GCS {}", blobPath);
        } catch (StorageException e) {
            LOGGER.error("Failed to create object in GCS {}", blobPath);
            String gcsErrorType = GCSErrorType.valueOfCode(e.getCode()).name();
            throw new BlobStorageException(gcsErrorType, "GCS Upload failed", e);
        }
    }
}
