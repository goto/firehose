package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.metrics.BlobStorageMetrics;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tracks a single in-flight blob storage upload and records its outcome.
 * <p>
 * Wraps the {@link Future} returned when a {@link BlobStorageWorker} is submitted, together with the
 * file's {@link LocalFileMetadata}. {@link #isFinished()} reports whether the upload has completed,
 * emitting success or failure metrics, and {@link #getFullPath()} exposes the uploaded file's path.
 * Lombok {@code @Data} generates the getters, setters, {@code equals}, {@code hashCode} and
 * {@code toString}.
 *
 * @see BlobStorageChecker
 * @see BlobStorageWorker
 */
@AllArgsConstructor
@Data
public class BlobStorageWriterFutureHandler {
    /** Future of the upload task, yielding the upload duration in milliseconds. */
    private Future<Long> future;
    /** Metadata of the file being uploaded. */
    private LocalFileMetadata localFileMetadata;
    /** Instrumentation used to emit upload success and failure metrics. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Placeholder used as the error type for non-blob-storage failures. */
    private static final String EMPTY = "";

    /**
     * Returns the full local path of the file being uploaded.
     *
     * @return the file path
     */
    public String getFullPath() {
        return localFileMetadata.getFullPath();
    }

    /**
     * Reports whether the upload has completed, recording metrics on completion.
     * <p>
     * Returns {@code false} while the upload is still running. On successful completion it captures
     * upload success metrics and returns {@code true}; on failure it captures failure metrics and
     * rethrows the cause as a {@link BlobStorageFailedException}.
     *
     * @return {@code true} if the upload completed successfully, {@code false} if still running
     * @throws BlobStorageFailedException if the upload was interrupted or failed
     */
    public boolean isFinished() {
        if (!future.isDone()) {
            return false;
        }
        try {
            long totalTime = future.get();
            captureFileUploadSuccessMetric(totalTime);
            return true;
        } catch (InterruptedException e) {
            captureUploadFailedMetric(e);
            throw new BlobStorageFailedException(e);
        } catch (ExecutionException e) {
            captureUploadFailedMetric(e.getCause());
            throw new BlobStorageFailedException(e.getCause());
        }
    }

    /**
     * Records metrics for a successful file upload.
     *
     * @param totalTime the upload duration in milliseconds
     */
    private void captureFileUploadSuccessMetric(long totalTime) {
        firehoseInstrumentation.logInfo("Flushed to blob storage {}", localFileMetadata.getFullPath());
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
        firehoseInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_BYTES, localFileMetadata.getSize());
        firehoseInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_RECORDS_TOTAL, localFileMetadata.getRecordCount());
        firehoseInstrumentation.captureDuration(BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    /**
     * Records metrics for a failed file upload, tagging the blob storage error type when known.
     *
     * @param e the cause of the failure
     */
    private void captureUploadFailedMetric(Throwable e) {
        firehoseInstrumentation.logError("Failed to flush to blob storage {}", e.getMessage());
        String errorType;
        if (e instanceof BlobStorageException) {
            errorType = ((BlobStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.FAILURE_TAG, Metrics.tag(BlobStorageMetrics.BLOB_STORAGE_ERROR_TYPE_TAG, errorType));
    }
}
