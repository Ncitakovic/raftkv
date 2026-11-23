package io.github.ncitakovic.raftkv.raft;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Production {@link Scheduler}: a single-threaded {@link ScheduledExecutorService}, whose one
 * thread is the node's Raft thread.
 */
public final class ExecutorScheduler implements Scheduler, AutoCloseable {

    private final ScheduledExecutorService executor;

    /**
     * @param executor must run tasks one at a time on a single thread, or Raft state could be
     *     changed concurrently; {@link #singleThread} creates one
     */
    public ExecutorScheduler(ScheduledExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /** A scheduler with its own daemon thread named {@code threadName}, e.g. {@code raft-node-1}. */
    public static ExecutorScheduler singleThread(String threadName) {
        Objects.requireNonNull(threadName, "threadName");
        return new ExecutorScheduler(Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, threadName);
            thread.setDaemon(true);
            return thread;
        }));
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        ScheduledFuture<?> future = executor.schedule(task, delay.toNanos(), TimeUnit.NANOSECONDS);
        return () -> future.cancel(false);
    }

    /** Stops the thread; tasks not yet started are dropped. */
    @Override
    public void close() {
        executor.shutdownNow();
    }
}
