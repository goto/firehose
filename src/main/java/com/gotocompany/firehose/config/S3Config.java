package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration for Amazon S3 (and S3-compatible) storage, used by the blob sink and the
 * blob-based DLQ writer.
 *
 * <p>Every key is prefixed with an {@code S3_TYPE} placeholder that owner expands at runtime to the
 * usage context (for example {@code SINK_BLOB} or {@code DLQ_BLOB_STORAGE}), so a single interface
 * can configure either destination. It supplies the region, bucket, object prefix and credentials
 * together with the client retry and timeout policy. Each accessor maps to an environment variable
 * via {@code @Key} and, where present, falls back to its {@code @DefaultValue}.
 */
public interface S3Config extends Config {
    /**
     * Returns the AWS region of the S3 bucket, set by ${S3_TYPE}_S3_REGION.
     *
     * @return the S3 region
     */
    @Key("${S3_TYPE}_S3_REGION")
    String getS3Region();

    /**
     * Returns the name of the S3 bucket objects are written to, set by ${S3_TYPE}_S3_BUCKET_NAME.
     *
     * @return the S3 bucket name
     */
    @Key("${S3_TYPE}_S3_BUCKET_NAME")
    String getS3BucketName();

    /**
     * Returns the object-name (directory) prefix prepended to uploaded objects, set by
     * ${S3_TYPE}_S3_DIRECTORY_PREFIX.
     *
     * @return the S3 object directory prefix
     */
    @Key("${S3_TYPE}_S3_DIRECTORY_PREFIX")
    String getS3DirectoryPrefix();

    /**
     * Returns the AWS access key id used to authenticate to S3, set by ${S3_TYPE}_S3_ACCESS_KEY.
     *
     * @return the S3 access key id
     */
    @Key("${S3_TYPE}_S3_ACCESS_KEY")
    String getS3AccessKey();

    /**
     * Returns the AWS secret access key used to authenticate to S3, set by ${S3_TYPE}_S3_SECRET_KEY.
     *
     * @return the S3 secret access key
     */
    @Key("${S3_TYPE}_S3_SECRET_KEY")
    String getS3SecretKey();

    /**
     * Returns the maximum number of retry attempts for an S3 operation, set by
     * ${S3_TYPE}_S3_RETRY_MAX_ATTEMPTS and defaulting to {@code 10}.
     *
     * @return the maximum S3 retry attempts
     */
    @Key("${S3_TYPE}_S3_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getS3RetryMaxAttempts();

    /**
     * Returns the base delay in milliseconds for the S3 retry backoff, set by
     * ${S3_TYPE}_S3_BASE_DELAY_MS and defaulting to {@code 1000}.
     *
     * @return the S3 retry base delay in milliseconds
     */
    @Key("${S3_TYPE}_S3_BASE_DELAY_MS")
    @DefaultValue("1000")
    Long getS3BaseDelay();

    /**
     * Returns the maximum backoff in milliseconds between S3 retries, set by
     * ${S3_TYPE}_S3_MAX_BACKOFF_MS and defaulting to {@code 30000}.
     *
     * @return the maximum S3 retry backoff in milliseconds
     */
    @Key("${S3_TYPE}_S3_MAX_BACKOFF_MS")
    @DefaultValue("30000")
    Long getS3MaxBackoff();

    /**
     * Returns the timeout in milliseconds for a single S3 API attempt, set by
     * ${S3_TYPE}_S3_API_ATTEMPT_TIMEOUT_MS and defaulting to {@code 10000}.
     *
     * @return the per-attempt S3 API timeout in milliseconds
     */
    @Key("${S3_TYPE}_S3_API_ATTEMPT_TIMEOUT_MS")
    @DefaultValue("10000")
    Long getS3ApiAttemptTimeout();

    /**
     * Returns the overall timeout in milliseconds for an S3 API call across all attempts, set by
     * ${S3_TYPE}_S3_API_TIMEOUT_MS and defaulting to {@code 40000}.
     *
     * @return the overall S3 API timeout in milliseconds
     */
    @Key("${S3_TYPE}_S3_API_TIMEOUT_MS")
    @DefaultValue("40000")
    Long getS3ApiTimeout();
}
