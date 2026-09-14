package com.gotocompany.firehose.launch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BootupTimer {
    private static final AtomicBoolean FIRST_CONSUMED = new AtomicBoolean(false);
    private static volatile long startNanos;

    private BootupTimer() {
    }

    public static void markProcessStart() {
        startNanos = System.nanoTime();
    }

    public static void markFirstConsumed() {
        if (startNanos == 0L) {
            return;
        }
        if (FIRST_CONSUMED.compareAndSet(false, true)) {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            System.out.println("Bootup time to first consumed batch: " + elapsedMs + " ms");
        }
    }
}

