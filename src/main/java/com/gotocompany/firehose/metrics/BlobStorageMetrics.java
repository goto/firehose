package com.gotocompany.firehose.metrics;


/**
 * Metric names and tags for the blob-storage (object-storage) sink.
 *
 * <p>Defines counters and timers for the local file lifecycle (open, close, records, size, closing
 * time) and remote upload (time, count, bytes, records), built on the shared {@link Metrics}
 * prefixes.
 */
public class BlobStorageMetrics {
    /** Counter of local files opened for writing. */
    public static final String LOCAL_FILE_OPEN_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_open_total";
    /** Counter of local files closed. */
    public static final String LOCAL_FILE_CLOSE_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_close_total";
    /** Counter of records written to local files. */
    public static final String LOCAL_FILE_RECORDS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_records_total";
    /** Timer for how long closing a local file takes, in milliseconds. */
    public static final String LOCAL_FILE_CLOSING_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_closing_time_milliseconds";
    /** Gauge of local file size, in bytes. */
    public static final String LOCAL_FILE_SIZE_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_size_bytes";
    /** Timer for how long uploading a file to remote storage takes, in milliseconds. */
    public static final String FILE_UPLOAD_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_time_milliseconds";
    /** Counter of files uploaded to remote storage. */
    public static final String FILE_UPLOAD_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_total";
    /** Counter of bytes uploaded to remote storage. */
    public static final String FILE_UPLOAD_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_bytes";
    /** Counter of records uploaded to remote storage. */
    public static final String FILE_UPLOAD_RECORDS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_records_total";

    /** Tag key carrying the blob-storage error type. */
    public static final String BLOB_STORAGE_ERROR_TYPE_TAG = "error_type";
}
