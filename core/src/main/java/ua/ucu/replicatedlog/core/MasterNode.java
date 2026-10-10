package ua.ucu.replicatedlog.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    /**
     * Appends the message locally, replicates it to every secondary in parallel and blocks until all of
     * them have ACKed (blocking replication, Iteration 1).
     */
    public void appendMsg(String payload) {
        log.info("Append received: '{}'", payload);
        Message message = storeLocally(payload);
        replicateToAll(message);
        log.info("Append finished for '{}'", payload);
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

    private void replicateToAll(Message message) {
        log.info("Replicating '{}' to {} secondaries in parallel", message.payload(), secondaries.size());
        long start = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = secondaries.stream()
                .map(secondary -> CompletableFuture.runAsync(() -> sendTo(secondary, message), executor))
                .toList();

        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof RuntimeException cause ? cause : e;
        }
        log.info("All {} ACKs received for '{}' in {} ms", secondaries.size(), message.payload(),
                System.currentTimeMillis() - start);
    }

    private void sendTo(NodeId secondary, Message message) {
        log.info("Sending '{}' to {}", message.payload(), secondary);
        transport.send(id, secondary, message);
        log.info("ACK received from {} for '{}'", secondary, message.payload());
    }
}
