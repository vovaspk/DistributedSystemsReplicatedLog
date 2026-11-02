package ua.ucu.replicatedlog.core;

/**
 * A replicated log entry. {@code seq} is assigned by the master and defines the total order (secondaries
 * expose messages strictly by ascending seq, without gaps); {@code id} will be used for deduplication in
 * Iteration 3.
 */
public record Message(String id, long seq, String payload) {
}
