package ua.ucu.replicatedlog.core.harness;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import ua.ucu.replicatedlog.core.Message;
import ua.ucu.replicatedlog.core.NodeId;
import ua.ucu.replicatedlog.core.SecondaryNode;
import ua.ucu.replicatedlog.core.Transport;
import ua.ucu.replicatedlog.core.TransportException;

/**
 * In-process transport for tests: delivers messages straight to registered {@link SecondaryNode}s and lets a
 * test inject faults per <b>directed</b> link ({@code from -> to}) via {@link LinkRule}s. Delaying
 * master -> secondary does not delay the opposite direction.
 */
public class MockedTransport implements Transport {

    private record Link(NodeId from, NodeId to) {
    }

    private final Map<NodeId, SecondaryNode> nodes = new ConcurrentHashMap<>();
    private final Map<Link, List<LinkRule>> rules = new ConcurrentHashMap<>();

    public void register(SecondaryNode node) {
        nodes.put(node.id(), node);
    }

    public void unregister(NodeId id) {
        nodes.remove(id);
    }

    public void setDelay(NodeId from, NodeId to, Duration delay) {
        removeDelay(from, to);
        addRule(from, to, new DelayRule(delay));
    }

    public void removeDelay(NodeId from, NodeId to) {
        rulesFor(new Link(from, to)).removeIf(rule -> rule instanceof DelayRule);
    }

    public void addRule(NodeId from, NodeId to, LinkRule rule) {
        rulesFor(new Link(from, to)).add(rule);
    }

    public void clearRules(NodeId from, NodeId to) {
        rulesFor(new Link(from, to)).clear();
    }

    @Override
    public void send(NodeId from, NodeId to, Message message) {
        SecondaryNode target = nodes.get(to);
        if (target == null) {
            throw new TransportException("Node " + to + " is not reachable");
        }
        deliver(rulesFor(new Link(from, to)).iterator(), message, () -> target.onReplicate(message));
    }

    private static void deliver(java.util.Iterator<LinkRule> chain, Message message, LinkRule.Delivery last) {
        if (chain.hasNext()) {
            chain.next().apply(message, () -> deliver(chain, message, last));
        } else {
            last.run();
        }
    }

    private List<LinkRule> rulesFor(Link link) {
        return rules.computeIfAbsent(link, l -> new CopyOnWriteArrayList<>());
    }
}
