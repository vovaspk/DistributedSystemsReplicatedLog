package ua.ucu.replicatedlog.core;

/**
 * Master-to-secondary communication channel. Implementations are blocking: returning normally means the
 * target node has stored the message and acknowledged it (ACK); a failure is reported with
 * {@link TransportException}.
 */
public interface Transport {

    void send(NodeId from, NodeId to, Message message);
}
