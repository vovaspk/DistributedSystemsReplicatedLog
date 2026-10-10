package ua.ucu.replicatedlog.core;

/** Identifies a node in the cluster (for the HTTP transport this is the node's base URL). */
public record NodeId(String value) {

    @Override
    public String toString() {
        return value;
    }
}
