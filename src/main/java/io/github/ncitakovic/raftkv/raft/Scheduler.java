package io.github.ncitakovic.raftkv.raft;

import java.time.Duration;

/**
 * The node's Raft thread, and its only source of time.
 *
 * <p>Every task given to a scheduler runs on one thread, one at a time, so Raft state needs no
 * locks. Time is injected through this interface: production uses a real executor
 * ({@link ExecutorScheduler}), and tests use a fake clock that is advanced by hand, without
 * {@code Thread.sleep} and without real concurrency.
 */
public interface Scheduler {

    /** Runs {@code task} on the Raft thread as soon as possible. */
    void execute(Runnable task);

    /** Runs {@code task} on the Raft thread after {@code delay}, unless it is cancelled first. */
    Cancellable schedule(Duration delay, Runnable task);
}
