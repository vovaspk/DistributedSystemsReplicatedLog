package ua.ucu.replicatedlog.core;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondaryNode {

    private static final Logger log = LoggerFactory.getLogger(SecondaryNode.class);

    private final NodeId id;
    private final Duration artificialDelay;
    private final List<Message> messages = new CopyOnWriteArrayList<>();

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

    /** Stores the replicated message; returning is the ACK. */
    public void onReplicate(Message message) {
        log.info("[{}] Replication request received: '{}', sleeping {} ms", id, message.payload(),
                artificialDelay.toMillis());
        sleep(artificialDelay);
        messages.add(message);
        log.info("[{}] Stored '{}', total messages: {}. Sending ACK", id, message.payload(), messages.size());
    }

    public List<String> listMsgs() {
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
