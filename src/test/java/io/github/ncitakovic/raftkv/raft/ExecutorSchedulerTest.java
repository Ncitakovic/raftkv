package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The production scheduler: one named thread, tasks in order, cancellation. Waits on latches, never sleeps. */
class ExecutorSchedulerTest {

    @Test
    void runsEveryTaskOnTheOneNamedThreadInOrder() throws Exception {
        try (ExecutorScheduler scheduler = ExecutorScheduler.singleThread("raft-test")) {
            List<String> seen = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);
            scheduler.execute(() -> seen.add("a:" + Thread.currentThread().getName()));
            scheduler.execute(() -> seen.add("b:" + Thread.currentThread().getName()));
            // Submitted last, so it runs after a and b whatever the delay.
            scheduler.schedule(Duration.ofMillis(1), () -> {
                seen.add("c:" + Thread.currentThread().getName());
                done.countDown();
            });

            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("a:raft-test", "b:raft-test", "c:raft-test"), seen);
        }
    }

    @Test
    void cancelledTaskDoesNotRun() throws Exception {
        try (ExecutorScheduler scheduler = ExecutorScheduler.singleThread("raft-test")) {
            List<String> seen = new CopyOnWriteArrayList<>();
            Cancellable timer = scheduler.schedule(Duration.ofMillis(20), () -> seen.add("cancelled"));
            timer.cancel();
            CountDownLatch done = new CountDownLatch(1);
            scheduler.schedule(Duration.ofMillis(40), done::countDown);

            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertTrue(seen.isEmpty());
        }
    }
}
