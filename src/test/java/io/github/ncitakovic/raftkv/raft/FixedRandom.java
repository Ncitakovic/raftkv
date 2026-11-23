package io.github.ncitakovic.raftkv.raft;

import java.util.random.RandomGenerator;

/**
 * A "random" source that returns the given values in turn (repeating the last one), so a test
 * knows every election timeout exactly. It supports only bounded draws, which is all
 * {@link RaftNode} uses.
 */
final class FixedRandom implements RandomGenerator {

    private final long[] values;
    private int next;

    FixedRandom(long... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("At least one value");
        }
        this.values = values.clone();
    }

    @Override
    public long nextLong(long origin, long bound) {
        long value = values[Math.min(next, values.length - 1)];
        next++;
        if (value < origin || value >= bound) {
            throw new IllegalStateException(value + " is outside [" + origin + ", " + bound + ")");
        }
        return value;
    }

    @Override
    public long nextLong() {
        throw new UnsupportedOperationException("RaftNode should draw bounded values only");
    }
}
