package com.gotocompany.firehose.exception;

/**
 * Unchecked exception signalling that an asynchronous sink task failed to complete.
 *
 * <p>It is raised when a sink is executed concurrently through a pool of worker tasks and one of
 * those tasks throws while pushing a batch, for example from {@code SinkPool}. The wrapped cause
 * carries the original failure so the consumer can decide whether to stop or recover.
 */
public class SinkTaskFailedException extends RuntimeException {
    /**
     * Wraps the failure thrown by an asynchronous sink task.
     *
     * @param throwable the original error raised by the sink task
     */
    public SinkTaskFailedException(Throwable throwable) {
        super(throwable);
    }
}
