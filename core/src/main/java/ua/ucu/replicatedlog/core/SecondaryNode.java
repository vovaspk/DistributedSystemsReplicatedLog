package ua.ucu.replicatedlog.core;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondaryNode {

    private static final Logger log = LoggerFactory.getLogger(SecondaryNode.class);

    private final NodeId id;
    private final Duration artificialDelay;
    private final List<Message> messages = new ArrayList<>();
    /** Messages received ahead of their turn, keyed by seq; shown only once the gap before them is filled. */
    private final TreeMap<Long, Message> pending = new TreeMap<>();
    private long nextExpectedSeq = 1;

    public SecondaryNode(NodeId id) {
        this(id, Duration.ZERO);
    }

    /** @param artificialDelay sleep applied on every replication request (used to demo blocking replication) */
    public SecondaryNode(NodeId id, Duration artificialDelay) {
        this.id = id;
        this.artificialDelay = artificialDelay;
        log.info("Secondary {} started with artificial replication delay {} ms", id, artificialDelay.toMillis());
    }

    public NodeId id() {
        return id;
    }

    /**
     * Stores the replicated message; returning is the ACK. Messages may arrive out of order (parallel
     * replication, reordering network), so they are buffered and exposed only as a gap-free prefix by
     * {@code seq}: total order is the same as on the master. Duplicates are ignored.
     */
    public void onReplicate(Message message) {
        log.info("[{}] Replication request received: '{}' (seq={}), sleeping {} ms", id, message.payload(),
                message.seq(), artificialDelay.toMillis());
        sleep(artificialDelay);
        synchronized (this) {
            if (message.seq() < nextExpectedSeq || pending.containsKey(message.seq())) {
                log.info("[{}] Duplicate '{}' (seq={}) ignored", id, message.payload(), message.seq());
            } else {
                pending.put(message.seq(), message);
                publishContiguous();
            }
        }
        log.info("[{}] Sending ACK for '{}' (seq={})", id, message.payload(), message.seq());
    }

    private void publishContiguous() {
        Message next;
        while ((next = pending.get(nextExpectedSeq)) != null) {
            pending.remove(nextExpectedSeq);
            messages.add(next);
            nextExpectedSeq++;
            log.info("[{}] Stored '{}' (seq={}), visible messages: {}", id, next.payload(), next.seq(),
                    messages.size());
        }
        if (!pending.isEmpty()) {
            log.info("[{}] Waiting for seq={}; buffered out-of-order seqs: {}", id, nextExpectedSeq,
                    pending.keySet());
        }
    }

    public synchronized List<String> listMsgs() {
        log.info("[{}] list -> {} messages", id, messages.size());
        return messages.stream().map(Message::payload).toList();
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException("Interrupted while processing replication request", e);
        }
    }
}
