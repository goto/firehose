package com.gotocompany.firehose.launch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

/**
 * The Task with parallelism.
 */
public class Task {

    /** Pool that runs the parallel task threads. */
    private final ExecutorService executorService;
    /** Number of threads (and the latch count) to run the task on. */
    private int parallelism;
    /** Delay, in milliseconds, to wait after cancelling threads during {@link #stop()}. */
    private int threadCleanupDelay;
    /** The work to run on each thread; receives the finish callback to invoke when done. */
    private Consumer<Runnable> task;
    /** Callback that counts down the latch when a task thread finishes. */
    private Runnable taskFinishCallback;
    /** Latch released once per thread, used to await completion of all threads. */
    private final CountDownLatch countDownLatch;
    /** Futures of the submitted task threads, used to cancel them on stop. */
    private final List<Future<?>> fnFutures;
    /** Records lifecycle logs for the task. */
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Task.
     *
     * @param parallelism             the parallelism
     * @param threadCleanupDelay      the thread cleanup delay
     * @param firehoseInstrumentation the instrumentation
     * @param task                    the task
     */
    public Task(int parallelism, int threadCleanupDelay, FirehoseInstrumentation firehoseInstrumentation, Consumer<Runnable> task) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.task = task;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        taskFinishCallback = countDownLatch::countDown;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    /**
     * Submits the task to run on all threads of the pool.
     *
     * @return this task, for chaining
     */
    public Task run() {
        for (int i = 0; i < parallelism; i++) {
            fnFutures.add(executorService.submit(() -> {
                task.accept(taskFinishCallback);
            }));
        }
        return this;
    }

    /**
     * Blocks until every task thread has signalled completion.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void waitForCompletion() throws InterruptedException {
        firehoseInstrumentation.logInfo("waiting for completion");
        countDownLatch.await();
    }

    /**
     * Cancels all running task threads and waits the configured cleanup delay.
     *
     * @return this task, for chaining
     */
    public Task stop() {
        try {
            firehoseInstrumentation.logInfo("Stopping task thread");
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            firehoseInstrumentation.logInfo("Sleeping thread during clean up for {} duration", threadCleanupDelay);
            Thread.sleep(threadCleanupDelay);
        } catch (InterruptedException e) {
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "error stopping tasks");
        }
        return this;
    }
}
