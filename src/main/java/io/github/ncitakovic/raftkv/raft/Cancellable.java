package io.github.ncitakovic.raftkv.raft;

/** A handle to a scheduled task. Cancelling a task that has already run does nothing. */
@FunctionalInterface
public interface Cancellable {
    void cancel();
}
