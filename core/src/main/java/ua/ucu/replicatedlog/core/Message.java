package ua.ucu.replicatedlog.core;

/**
 * A replicated log entry. {@code id} and {@code seq} are assigned by the master; they are not used by
 * Iteration 1 yet, but ordering (Iteration 2) and deduplication (Iteration 3) will rely on them.
 */
public record Message(String id, long seq, String payload) {
}
