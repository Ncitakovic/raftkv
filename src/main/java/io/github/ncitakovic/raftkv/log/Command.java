package io.github.ncitakovic.raftkv.log;

import java.util.Arrays;
import java.util.Objects;

/**
 * A state-changing operation recorded in the log.
 *
 * <p>Commands are the unit of replication: in later milestones the leader appends a command
 * to its Raft log, replicates it to a majority, and only then applies it to the state machine.
 */
public record Command(Type type, String key, byte[] value) {

    public enum Type {
        PUT((byte) 1),
        DELETE((byte) 2);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (Type t : values()) {
                if (t.code == code) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Unknown command type: " + code);
        }
    }

    public Command {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        value = value == null ? new byte[0] : value.clone();
    }

    public static Command put(String key, byte[] value) {
        return new Command(Type.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(Type.DELETE, key, null);
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Command c
                && type == c.type
                && key.equals(c.key)
                && Arrays.equals(value, c.value);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(type, key) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Command[" + type + " " + key + " (" + value.length + " bytes)]";
    }
}
