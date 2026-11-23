package io.github.ncitakovic.raftkv.raft;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A {@link Scheduler} with a hand-driven clock, for tests. Nothing runs until the test calls
 * {@link #advance} or {@link #runPending}; tasks then run on the test's own thread, in order of
 * due time and, for equal times, in the order they were scheduled. No sleeping, no other threads.
 */
final class FakeScheduler implements Scheduler {

    private static final class Task {
        final long dueAt;
        final long seq;
        final Runnable action;
        boolean cancelled;

        Task(long dueAt, long seq, Runnable action) {
            this.dueAt = dueAt;
            this.seq = seq;
            this.action = action;
        }
    }

    private final PriorityQueue<Task> queue =
            new PriorityQueue<>(Comparator.<Task>comparingLong(t -> t.dueAt).thenComparingLong(t -> t.seq));
    private long now;
    private long nextSeq;

    @Override
    public void execute(Runnable task) {
        add(now, task);
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Task t = add(now + delay.toMillis(), task);
        return () -> t.cancelled = true;
    }

    /** Moves the clock forward by {@code ms}, running every task that falls due, including new ones. */
    void advance(long ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("Time only moves forward: " + ms);
        }
        long target = now + ms;
        while (!queue.isEmpty() && queue.peek().dueAt <= target) {
            Task t = queue.poll();
            now = t.dueAt;
            if (!t.cancelled) {
                t.action.run();
            }
        }
        now = target;
    }

    /** Runs everything due now, such as RPCs and responses handed to the Raft thread. */
    void runPending() {
        advance(0);
    }

    long now() {
        return now;
    }

    private Task add(long dueAt, Runnable action) {
        Task t = new Task(dueAt, nextSeq++, action);
        queue.add(t);
        return t;
    }
}
