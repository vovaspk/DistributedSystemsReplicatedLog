package ua.ucu.replicatedlog.core.harness;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import ua.ucu.replicatedlog.core.MasterNode;
import ua.ucu.replicatedlog.core.NodeId;
import ua.ucu.replicatedlog.core.SecondaryNode;

/** Wires a master and N secondaries over a {@link MockedTransport}, the starting point of every scenario. */
public class TestCluster {

    public final MockedTransport transport = new MockedTransport();
    public final NodeId masterId = new NodeId("master");
    public final List<SecondaryNode> secondaries = new ArrayList<>();
    public final MasterNode master;

    private TestCluster(int secondaryCount) {
        List<NodeId> ids = new ArrayList<>();
        for (int i = 1; i <= secondaryCount; i++) {
            SecondaryNode node = new SecondaryNode(new NodeId("s" + i));
            secondaries.add(node);
            transport.register(node);
            ids.add(node.id());
        }
        master = new MasterNode(masterId, transport, ids);
    }

    public static TestCluster withSecondaries(int count) {
        return new TestCluster(count);
    }

    /** 1-based, like the assignment's S1, S2. */
    public SecondaryNode secondary(int number) {
        return secondaries.get(number - 1);
    }

    /** Appends on a separate thread so tests can observe blocking clients (Iterations 2 and 3). */
    public CompletableFuture<Void> appendAsync(String payload) {
        return CompletableFuture.runAsync(() -> master.appendMsg(payload), runnable -> Thread.ofVirtual().start(runnable));
    }
}
