package com.gotocompany.firehose.sink.blob.writer.local;

import com.gotocompany.firehose.metrics.BlobStorageMetrics;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Scheduled task that rotates local files whose writers have met a rotation policy.
 * <p>
 * Run periodically by the writer orchestrator, it inspects the open {@link LocalFileWriter}
 * instances, closes those that {@link LocalStorage#shouldRotate(LocalFileWriter)} flags for rotation,
 * and enqueues each closed file's {@link LocalFileMetadata} for upload to blob storage. It also emits
 * open and closed file counts, closing latency, file size and record-count metrics.
 *
 * @see com.gotocompany.firehose.sink.blob.writer.WriterOrchestrator
 * @see LocalStorage
 */
public class LocalFileChecker implements Runnable {
    /** Queue of closed-file metadata awaiting upload to blob storage. */
    private final Queue<LocalFileMetadata> toBeFlushedToRemotePaths;
    /** Map of open writers keyed by time-partition path, shared with the orchestrator. */
    private final Map<Path, LocalFileWriter> timePartitionWriterMap;
    /** Local storage used to evaluate rotation policies. */
    private final LocalStorage localStorage;
    /** Instrumentation used to emit local-file rotation metrics. */
    private final FirehoseInstrumentation firehoseInstrumentation;


    /**
     * Creates a local file checker.
     *
     * @param toBeFlushedToRemotePaths the queue to which closed-file metadata is added for upload
     * @param timePartitionWriterMap the shared map of open writers keyed by partition path
     * @param localStorage the local storage used to decide which writers should rotate
     * @param firehoseInstrumentation the instrumentation used to publish metrics
     */
    public LocalFileChecker(Queue<LocalFileMetadata> toBeFlushedToRemotePaths,
                            Map<Path, LocalFileWriter> timePartitionWriterMap,
                            LocalStorage localStorage,
                            FirehoseInstrumentation firehoseInstrumentation) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.localStorage = localStorage;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Closes and enqueues all local files that are due for rotation.
     * <p>
     * Selects the writers that {@link LocalStorage#shouldRotate(LocalFileWriter)} reports as ready,
     * removes them from the shared writer map, closes each one and adds its metadata to the upload
     * queue, recording success or failure metrics.
     *
     * @throws LocalFileWriterFailedException if closing a local file fails
     */
    @Override
    public void run() {
        firehoseInstrumentation.captureValue(BlobStorageMetrics.LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
        Map<Path, LocalFileWriter> toBeRotated =
                timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        timePartitionWriterMap.entrySet().removeAll(toBeRotated.entrySet());
        toBeRotated.forEach((path, writer) -> {
            try {
                Instant startTime = Instant.now();
                LocalFileMetadata metadata = writer.closeAndFetchMetaData();
                firehoseInstrumentation.logInfo("Closing Local File {} ", metadata.getFullPath());
                toBeFlushedToRemotePaths.add(metadata);
                captureFileClosedSuccessMetric(startTime, metadata);
            } catch (IOException e) {
                e.printStackTrace();
                captureFileCloseFailedMetric();
                throw new LocalFileWriterFailedException(e);
            }
        });
        firehoseInstrumentation.captureValue(BlobStorageMetrics.LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
    }

    /**
     * Records metrics for a successfully closed local file.
     *
     * @param startTime the instant the file began closing, used to measure latency
     * @param localFileMetadata the metadata of the closed file
     */
    private void captureFileClosedSuccessMetric(Instant startTime, LocalFileMetadata localFileMetadata) {
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.LOCAL_FILE_CLOSE_TOTAL, Metrics.SUCCESS_TAG);
        firehoseInstrumentation.captureDurationSince(BlobStorageMetrics.LOCAL_FILE_CLOSING_TIME_MILLISECONDS, startTime);
        firehoseInstrumentation.captureCount(BlobStorageMetrics.LOCAL_FILE_SIZE_BYTES, localFileMetadata.getSize());
        firehoseInstrumentation.captureCount(BlobStorageMetrics.LOCAL_FILE_RECORDS_TOTAL, localFileMetadata.getRecordCount());
    }

    /**
     * Records a metric for a local file that failed to close.
     */
    private void captureFileCloseFailedMetric() {
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.LOCAL_FILE_CLOSE_TOTAL, Metrics.FAILURE_TAG);
    }
}
