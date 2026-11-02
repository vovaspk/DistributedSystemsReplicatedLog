package ua.ucu.replicatedlog.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterNode {

    private static final Logger log = LoggerFactory.getLogger(MasterNode.class);

    private final NodeId id;
    private final Transport transport;
    private final List<NodeId> secondaries;
    private final List<Message> messages = new ArrayList<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private long nextSeq = 1;

    public MasterNode(NodeId id, Transport transport, List<NodeId> secondaries) {
        this.id = id;
        this.transport = transport;
        this.secondaries = List.copyOf(secondaries);
        log.info("Master {} configured with secondaries: {}", id, this.secondaries);
    }

    /** Appends with the strictest write concern: the master and every secondary (Iteration 1 behaviour). */
    public void appendMsg(String payload) {
        appendMsg(payload, secondaries.size() + 1);
    }

    /**
     * Appends the message locally and replicates it to every secondary in parallel, but answers the caller
     * as soon as {@code w - 1} secondaries have ACKed (write concern: {@code w = 1} is the master alone,
     * {@code w = 2} the master and one secondary, ...). Replication to the remaining secondaries continues in
     * the background.
     */
    public void appendMsg(String payload, int w) {
        int maxW = secondaries.size() + 1;
        if (w < 1 || w > maxW) {
            throw new IllegalArgumentException("Write concern w must be between 1 and " + maxW + " but was " + w);
        }
        log.info("Append received: '{}' (w={})", payload, w);
        Message message = storeLocally(payload);
        AckTracker tracker = new AckTracker(w - 1, secondaries.size());
        replicateToAll(message, tracker);
        tracker.await();
        log.info("Append finished for '{}' (w={})", payload, w);
    }

    public List<String> listMsgs() {
        synchronized (messages) {
            log.info("Master list -> {} messages", messages.size());
            return messages.stream().map(Message::payload).toList();
        }
    }

    private Message storeLocally(String payload) {
        synchronized (messages) {
            Message message = new Message(UUID.randomUUID().toString(), nextSeq++, payload);
            messages.add(message);
            log.info("Stored '{}' locally (seq={}), total messages: {}", payload, message.seq(), messages.size());
            return message;
        }
    }

    private void replicateToAll(Message message, AckTracker tracker) {
        log.info("Replicating '{}' (seq={}) to {} secondaries in parallel", message.payload(), message.seq(),
                secondaries.size());
        for (NodeId secondary : secondaries) {
            executor.execute(() -> sendTo(secondary, message, tracker));
        }
    }

    private void sendTo(NodeId secondary, Message message, AckTracker tracker) {
        long start = System.currentTimeMillis();
        log.info("Sending '{}' (seq={}) to {}", message.payload(), message.seq(), secondary);
        try {
            transport.send(id, secondary, message);
        } catch (RuntimeException e) {
            log.error("Replication of '{}' to {} failed: {}", message.payload(), secondary, e.getMessage());
            tracker.failure();
            return;
        }
        log.info("ACK received from {} for '{}' in {} ms", secondary, message.payload(),
                System.currentTimeMillis() - start);
        tracker.ack();
    }

    /**
     * Counts ACKs of one append. Iteration 2 assumes a perfect link, so a failure is only tracked to avoid
     * waiting forever for an ACK that can no longer arrive (retries come in Iteration 3).
     */
    private static final class AckTracker {

        private final int required;
        private int acks;
        private int failures;
        private final int total;

        AckTracker(int required, int total) {
            this.required = required;
            this.total = total;
        }

        synchronized void ack() {
            acks++;
            notifyAll();
        }

        synchronized void failure() {
            failures++;
            notifyAll();
        }

        synchronized void await() {
            try {
                while (acks < required) {
                    if (total - failures < required) {
                        throw new TransportException("Write concern cannot be satisfied: " + acks + " ACKs, "
                                + failures + " failed replications, " + required + " required");
                    }
                    wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransportException("Interrupted while waiting for ACKs", e);
            }
        }
    }
}
