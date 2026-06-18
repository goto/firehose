package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Scheduled task that uploads closed local files to blob storage and tracks their progress.
 * <p>
 * Run periodically by the writer orchestrator, it drains the queue of files awaiting upload, submits a
 * {@link BlobStorageWorker} for each to a thread pool (wrapping each in a
 * {@link BlobStorageWriterFutureHandler}), and moves the paths of completed uploads onto the
 * flushed-paths queue so their Kafka offsets can be committed and the local files deleted.
 *
 * @see BlobStorageWorker
 * @see BlobStorageWriterFutureHandler
 */
@AllArgsConstructor
public class BlobStorageChecker implements Runnable {

    /** Queue of closed local files awaiting upload, fed by the local file checker. */
    private final BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths;
    /** Queue of paths successfully uploaded, consumed by the orchestrator for offset commits. */
    private final BlockingQueue<String> flushedToRemotePaths;
    /** In-flight upload futures being tracked for completion. */
    private final Set<BlobStorageWriterFutureHandler> remoteUploadFutures;
    /** Thread pool that runs the upload workers. */
    private final ExecutorService remoteUploadScheduler;
    /** The blob storage backend files are uploaded to. */
    private final BlobStorage blobStorage;
    /** Instrumentation used to emit upload metrics. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Submits pending uploads and harvests completed ones.
     * <p>
     * Drains the files awaiting upload and submits an upload task for each, then collects the tasks
     * that have finished and publishes their paths to the flushed-paths queue.
     */
    @Override
    public void run() {
        List<LocalFileMetadata> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream().map(this::submitTask).collect(Collectors.toList()));
        Set<BlobStorageWriterFutureHandler> flushed = remoteUploadFutures.stream().filter(BlobStorageWriterFutureHandler::isFinished).collect(Collectors.toSet());
        remoteUploadFutures.removeAll(flushed);
        flushedToRemotePaths.addAll(flushed.stream().map(BlobStorageWriterFutureHandler::getFullPath).collect(Collectors.toSet()));
    }

    /**
     * Submits an upload task for a single local file.
     *
     * @param localFileMetadata the metadata of the file to upload
     * @return a handler tracking the submitted upload future
     */
    private BlobStorageWriterFutureHandler submitTask(LocalFileMetadata localFileMetadata) {
        BlobStorageWorker worker = new BlobStorageWorker(blobStorage, localFileMetadata);
        Future<Long> f = remoteUploadScheduler.submit(worker);
        return new BlobStorageWriterFutureHandler(f, localFileMetadata, firehoseInstrumentation);
    }
}

