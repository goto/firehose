package com.gotocompany.firehose.sink.blob.writer;

import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileWriter;
import com.gotocompany.firehose.sink.blob.writer.local.path.TimePartitionedPathUtils;
import com.gotocompany.firehose.sink.blob.writer.remote.BlobStorageChecker;
import com.gotocompany.firehose.sink.blob.writer.remote.BlobStorageWriterFutureHandler;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileChecker;
import com.gotocompany.firehose.sink.blob.writer.local.LocalStorage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class manages threads for local and blob storage checking.
 * It provides apis to write records to correct path based on time partitions.
 * <p>
 * LocalFileChecker: This thread is responsible for rotation of files based on policies.
 * Once a file is written to disk it adds to a queue to be consumed by ObjectStorageChecker.
 * <p>
 * ObjectStorageChecker: Reads the Local Files and Writes to given ObjectStorage.
 * After the file is written to blob storage, it adds to to flushedPath queue.
 */
public class WriterOrchestrator implements Closeable {
    /** Initial delay before the background checker threads first run, in seconds. */
    private static final int FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS = 10;
    /** Interval between background checker thread runs, in seconds. */
    private static final int FILE_CHECKER_THREAD_FREQUENCY_SECONDS = 5;
    /** Active local file writers keyed by their time-partition path. */
    private final Map<Path, LocalFileWriter> timePartitionWriterMap = new ConcurrentHashMap<>();
    /** Scheduler running the {@link LocalFileChecker} that rotates local files. */
    private final ScheduledExecutorService localFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    /** Scheduler running the {@link BlobStorageChecker} that uploads files to blob storage. */
    private final ScheduledExecutorService objectStorageCheckerScheduler = Executors.newScheduledThreadPool(1);
    /** Bounded pool that performs the actual remote uploads. */
    private final ExecutorService remoteUploadScheduler = Executors.newFixedThreadPool(10);
    /** Paths successfully flushed to blob storage, awaiting offset commit and local cleanup. */
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    /** Local storage abstraction used to create writers and delete local files. */
    private final LocalStorage localStorage;
    /** Tracks the health of the background checker threads. */
    private final WriterOrchestratorStatus writerOrchestratorStatus;
    /** Blob sink configuration, used to compute time-partitioned paths. */
    private final BlobSinkConfig sinkConfig;

    /**
     * Creates the orchestrator and starts its local and remote background workers.
     * <p>
     * Schedules a {@link LocalFileChecker} to rotate local files and a {@link BlobStorageChecker} to
     * upload closed files to the given blob storage, then begins monitoring their status.
     *
     * @param sinkConfig the blob sink configuration
     * @param localStorage the local storage used to create writers and manage local files
     * @param blobStorage the remote blob storage to upload files to
     * @param statsDReporter the reporter used to publish metrics for the background workers
     */
    public WriterOrchestrator(BlobSinkConfig sinkConfig, LocalStorage localStorage, BlobStorage blobStorage, StatsDReporter statsDReporter) {
        this.localStorage = localStorage;
        this.sinkConfig = sinkConfig;
        BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
        ScheduledFuture<?> localWriterFuture = localFileCheckerScheduler.scheduleAtFixedRate(
                new LocalFileChecker(
                        toBeFlushedToRemotePaths,
                        timePartitionWriterMap,
                        localStorage, new FirehoseInstrumentation(statsDReporter, LocalFileChecker.class)),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        Set<BlobStorageWriterFutureHandler> remoteUploadFutures = new HashSet<>();
        ScheduledFuture<?> objectStorageWriterFuture = objectStorageCheckerScheduler.scheduleWithFixedDelay(
                new BlobStorageChecker(
                        toBeFlushedToRemotePaths,
                        flushedToRemotePaths,
                        remoteUploadFutures,
                        remoteUploadScheduler,
                        blobStorage,
                        new FirehoseInstrumentation(statsDReporter, BlobStorageChecker.class)),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        writerOrchestratorStatus = new WriterOrchestratorStatus(localWriterFuture, objectStorageWriterFuture);
        writerOrchestratorStatus.startCheckers();
    }

    /**
     * @return Return all paths which are flushed to remote and drain the list.
     * It also cleans up local paths from the disk.
     */
    public Set<String> getFlushedPaths() {
        Set<String> flushedPaths = new HashSet<>();
        flushedToRemotePaths.drainTo(flushedPaths);
        flushedPaths.forEach(localStorage::deleteLocalFile);
        return flushedPaths;
    }

    /**
     * Verifies that the background workers are still healthy.
     *
     * @throws Exception if a background worker has failed and the orchestrator is closed
     */
    private void checkStatus() throws Exception {
        if (writerOrchestratorStatus.isClosed()) {
            throw new IOException(writerOrchestratorStatus.getThrowable());
        }
    }

    /**
     * Writes the records based on the partition configuration.
     *
     * @param record record to be written
     * @return Local path where the record was stored.
     * @throws Exception if local storage fails or writer orchestrator is closed.
     */
    public String write(Record record) throws Exception {
        checkStatus();
        Path timePartitionedPath = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        return write(record, timePartitionedPath);
    }

    /**
     * Tries to fetch writer from the map, if the writer is closed, try recursive method call.
     *
     * @param record              record to write
     * @param timePartitionedPath partition for the file path
     * @return full path of file.
     * @throws IOException if local storage fails.
     */
    private String write(Record record, Path timePartitionedPath) throws IOException {
        LocalFileWriter writer = timePartitionWriterMap.computeIfAbsent(
                timePartitionedPath,
                x -> localStorage.createLocalFileWriter(timePartitionedPath));
        if (!writer.write(record)) {
            return write(record, timePartitionedPath);
        }
        return writer.getFullPath();
    }

    /**
     * Stops the background workers and releases all local writers.
     * <p>
     * Shuts down the local-file, object-storage and upload schedulers, marks the status closed,
     * closes every open {@link LocalFileWriter} and deletes the remaining local files.
     *
     * @throws IOException if closing a writer fails
     */
    @Override
    public void close() throws IOException {
        localFileCheckerScheduler.shutdown();
        objectStorageCheckerScheduler.shutdown();
        remoteUploadScheduler.shutdown();
        writerOrchestratorStatus.setClosed(true);
        writerOrchestratorStatus.close();
        for (LocalFileWriter writer : timePartitionWriterMap.values()) {
            writer.close();
        }
        for (LocalFileWriter p : timePartitionWriterMap.values()) {
            localStorage.deleteLocalFile(p.getFullPath());
        }
    }
}
