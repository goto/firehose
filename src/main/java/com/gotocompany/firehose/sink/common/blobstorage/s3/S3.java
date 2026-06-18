package com.gotocompany.firehose.sink.common.blobstorage.s3;


import com.gotocompany.firehose.config.S3Config;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * {@link BlobStorage} implementation backed by Amazon S3.
 *
 * <p>Wraps an AWS SDK {@link S3Client} configured from {@link S3Config} (region, full-jitter retry policy and
 * API timeouts). On construction it verifies the target bucket is reachable. Objects are written under an
 * optional directory prefix; failures are translated into {@link BlobStorageException}.
 */
public class S3 implements BlobStorage {
    /** Logger for bucket checks and upload outcomes. */
    private static final Logger LOGGER = LoggerFactory.getLogger(S3.class);
    /** AWS SDK S3 client used to perform the uploads. */
    private final S3Client s3Client;
    /** Bound S3 configuration (bucket, region, prefix and retry settings). */
    private final S3Config s3Config;

    /**
     * Creates an S3 storage client from configuration and verifies the bucket exists.
     *
     * <p>Builds an {@code S3Client} with the configured region, full-jitter retry policy and API timeouts.
     *
     * @param s3Config the bound S3 configuration
     * @throws IllegalArgumentException if the configured bucket cannot be found or accessed
     */
    public S3(S3Config s3Config) {
        this(s3Config, S3Client.builder()
                .region(Region.of(s3Config.getS3Region()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder()
                                .numRetries(s3Config.getS3RetryMaxAttempts())
                                .backoffStrategy(FullJitterBackoffStrategy.builder()
                                        .baseDelay(Duration.ofMillis(s3Config.getS3BaseDelay()))
                                        .maxBackoffTime(Duration.ofMillis(s3Config.getS3MaxBackoff()))
                                        .build())
                                .build())
                        .apiCallTimeout(Duration.ofMillis(s3Config.getS3ApiTimeout()))
                        .apiCallAttemptTimeout(Duration.ofMillis(s3Config.getS3ApiAttemptTimeout()))
                        .build())
                .build());
        checkBucket();
    }

    /**
     * Creates an S3 storage client using a caller-supplied client, primarily for testing.
     *
     * @param s3Config the bound S3 configuration
     * @param s3Client the S3 client to use
     */
    public S3(S3Config s3Config, S3Client s3Client) {
        this.s3Client = s3Client;
        this.s3Config = s3Config;
    }

    /**
     * Verifies that the configured bucket exists and is accessible.
     *
     * @throws IllegalArgumentException if the bucket is missing or cannot be accessed
     */
    private void checkBucket() {
        String bucketName = s3Config.getS3BucketName();
        try {
            final HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            s3Client.headBucket(request);
            LOGGER.info("Bucket found " + bucketName);
        } catch (NoSuchBucketException ex) {
            LOGGER.error("Bucket not found " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);
        } catch (S3Exception ex) {
            LOGGER.error("Cannot check access " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);
        } catch (Exception ex) {
            LOGGER.error("Cannot check access", ex);
            throw ex;
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
     * Uploads the given bytes under the given object name, applying the directory prefix.
     *
     * @param objectName the destination object name, before applying the directory prefix
     * @param content    the bytes to upload
     * @throws BlobStorageException if the upload fails
     */
    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        String finalPath = createPath(objectName);
        try {
            PutObjectRequest putObject = PutObjectRequest.builder()
                    .bucket(s3Config.getS3BucketName())
                    .key(finalPath)
                    .build();
            s3Client.putObject(putObject, RequestBody.fromBytes(content));
            LOGGER.info("Created object in S3 {}", objectName);
        } catch (SdkServiceException | SdkClientException ase) {
            LOGGER.error("Failed to create object in S3 {}", objectName);
            throw new BlobStorageException(ase.getMessage(), ase.getMessage(), ase);
        }
    }

    /**
     * Prepends the configured directory prefix to the object name, when set.
     *
     * @param objectName the base object name
     * @return the full object key including any directory prefix
     */
    private String createPath(String objectName) {
        String prefix = s3Config.getS3DirectoryPrefix();
        return prefix == null || prefix.isEmpty()
                ? objectName : Paths.get(prefix, objectName).toString();
    }
}
