package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import static com.gotocompany.firehose.metrics.Metrics.RETRY_SLEEP_TIME_MILLISECONDS;
import static java.lang.Math.toIntExact;

/**
 * ExponentialBackOffProvider is a provider of exponential back-off algorithm.
 * <p>
 * The backoff time is computed as per the following formula.
 * Min{maximumExpiryTimeInMS, initialExpiryTimeInMs * Math.pow(backoffRate,
 * attemptCount)}
 */
public class ExponentialBackOffProvider implements BackOffProvider {

    /** Base delay applied before the first retry, in milliseconds. */
    private final int initialExpiryTimeInMs;
    /** Multiplier applied per attempt to grow the delay exponentially. */
    private final int backoffRate;
    /** Upper bound on the computed delay, in milliseconds. */
    private final int maximumExpiryTimeInMS;
    /** Records the sleep-time metric and debug logs for each back-off. */
    private FirehoseInstrumentation firehoseInstrumentation;
    /** Performs the actual thread sleep for the computed delay. */
    private final BackOff backOff;

    /**
     * Instantiates a new Exponential back off provider.
     *
     * @param initialExpiryTimeInMs the initial expiry time in ms
     * @param backoffRate           the backoff rate
     * @param maximumExpiryTimeInMS the maximum expiry time in ms
     * @param firehoseInstrumentation       the instrumentation
     * @param backOff               the back off
     */
    public ExponentialBackOffProvider(int initialExpiryTimeInMs, int backoffRate, int maximumExpiryTimeInMS,
                                      FirehoseInstrumentation firehoseInstrumentation, BackOff backOff) {
        this.initialExpiryTimeInMs = initialExpiryTimeInMs;
        this.backoffRate = backoffRate;
        this.maximumExpiryTimeInMS = maximumExpiryTimeInMS;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.backOff = backOff;
    }

    /**
     * Computes the delay for the given attempt, records it, and sleeps for that duration.
     *
     * @param attemptCount the current attempt count used to scale the delay
     */
    @Override
    public void backOff(int attemptCount) {
        long sleepTime = this.calculateDelay(attemptCount);
        firehoseInstrumentation.logDebug("Backing off for {} milliseconds before retry attempt {}", sleepTime, attemptCount + 1);
        firehoseInstrumentation.captureSleepTime(RETRY_SLEEP_TIME_MILLISECONDS, toIntExact(sleepTime));
        backOff.inMilliSeconds(sleepTime);
    }

    /**
     * Computes the exponential delay for an attempt, capped at the configured maximum.
     *
     * @param attemptCount the current attempt count
     * @return the delay to wait, in milliseconds
     */
    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
