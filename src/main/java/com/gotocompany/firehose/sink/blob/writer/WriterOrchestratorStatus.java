package com.gotocompany.firehose.sink.blob.writer;

import lombok.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * Tracks the liveness of the {@link WriterOrchestrator}'s background workers.
 * <p>
 * Holds the scheduled futures of the local-file and object-storage checker tasks and starts two
 * watcher threads that block on those futures. If a worker stops, normally or because of an error,
 * the watcher records the cause in {@code throwable} and marks the status closed, which the
 * orchestrator uses to fail subsequent writes. Lombok {@code @Data} generates the getters, setters,
 * {@code equals}, {@code hashCode} and {@code toString}.
 *
 * @see WriterOrchestrator
 */
@Data
public class WriterOrchestratorStatus implements Closeable {
    /** Whether the orchestrator has been closed because a worker stopped or failed. */
    private boolean isClosed;
    /** Future of the scheduled local-file checker task. */
    private ScheduledFuture<?> localFileWriterFuture;
    /** Future of the scheduled object-storage checker task. */
    private ScheduledFuture<?> objectStorageWriterFuture;
    /** The error that caused a worker to stop, if any. */
    private Throwable throwable;
    /** Watcher thread that waits for the local-file checker to finish. */
    private Thread localFileWriterCompletionChecker;
    /** Watcher thread that waits for the object-storage checker to finish. */
    private Thread objectStorageWriterCompletionChecker;

    /**
     * Creates a status holder for the two background worker futures.
     *
     * @param localFileWriterFuture the future of the local-file checker task
     * @param objectStorageWriterFuture the future of the object-storage checker task
     */
    public WriterOrchestratorStatus(ScheduledFuture<?> localFileWriterFuture, ScheduledFuture<?> objectStorageWriterFuture) {
        this.localFileWriterFuture = localFileWriterFuture;
        this.objectStorageWriterFuture = objectStorageWriterFuture;
    }

    /**
     * Starts the watcher threads that monitor the two background workers.
     * <p>
     * Each watcher blocks on its worker's future; when the future completes or fails the cause is
     * recorded and the status is marked closed.
     */
    public void startCheckers() {
        localFileWriterCompletionChecker = new Thread(() -> {
            try {
                getLocalFileWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        });
        objectStorageWriterCompletionChecker = new Thread(() -> {
            try {
                getObjectStorageWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        });
        localFileWriterCompletionChecker.start();
        objectStorageWriterCompletionChecker.start();
    }

    /**
     * Interrupts the watcher threads so they stop monitoring the background workers.
     *
     * @throws IOException declared by {@link Closeable}; not thrown by this implementation
     */
    @Override
    public void close() throws IOException {
        localFileWriterCompletionChecker.interrupt();
        objectStorageWriterCompletionChecker.interrupt();
    }
}
