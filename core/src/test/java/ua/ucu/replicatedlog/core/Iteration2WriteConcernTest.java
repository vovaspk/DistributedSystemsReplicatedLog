package ua.ucu.replicatedlog.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ua.ucu.replicatedlog.core.harness.TestTime.TOLERANCE;
import static ua.ucu.replicatedlog.core.harness.TestTime.sec;
import static ua.ucu.replicatedlog.core.harness.Timing.assertBlocksAbout;
import static ua.ucu.replicatedlog.core.harness.Timing.assertCompletesWithin;
import static ua.ucu.replicatedlog.core.harness.Timing.awaitEventually;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ua.ucu.replicatedlog.core.harness.TestCluster;

/**
 * Iteration 2: tunable write concern, eventual consistency of a slow secondary, total order under
 * parallel replication and under a reordering network.
 */
class Iteration2WriteConcernTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = TestCluster.withSecondaries(2);
    }

    /** The assignment's acceptance test: S2 is slow (7 s in assignment terms, scaled by TestTime.SCALE). */
    @Test
    void acceptanceScenario() {
        var s1 = cluster.secondary(1);
        var s2 = cluster.secondary(2);
        cluster.transport.setDelay(cluster.masterId, s2.id(), sec(7));

        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("Msg1", 1));
        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("Msg2", 2));

        long start = System.nanoTime();
        CompletableFuture<Void> msg3 = cluster.appendAsync("Msg3", 3);
        assertFalse(msg3.isDone(), "Msg3 (w=3) must wait for the slow S2");

        // Msg4 (w=1) is not blocked by the pending Msg3
        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("Msg4", 1));
        assertFalse(msg3.isDone(), "Msg3 must still be waiting after Msg4 returned");

        List<String> all = List.of("Msg1", "Msg2", "Msg3", "Msg4");
        assertEquals(all, cluster.master.listMsgs());
        awaitEventually(() -> s1.listMsgs().equals(all), EVENTUALLY);

        // S2 temporarily lags behind: it shows a prefix of the log, never Msg4 ahead of Msg3
        List<String> s2Now = s2.listMsgs();
        assertTrue(all.subList(0, s2Now.size()).equals(s2Now) && s2Now.size() < all.size(),
                "S2 must be a strict prefix of the log while it is delayed, was " + s2Now);

        msg3.join();
        Duration blocked = Duration.ofNanos(System.nanoTime() - start);
        assertTrue(blocked.compareTo(sec(7)) >= 0, "Msg3 returned too early: " + blocked.toMillis() + " ms");

        // eventual consistency: S2 catches up
        awaitEventually(() -> s2.listMsgs().equals(all), EVENTUALLY);
    }

    @Test
    void w1DoesNotWaitForAnySecondary() {
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), sec(5));
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(2).id(), sec(6));

        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("m1", 1));

        assertEquals(List.of("m1"), cluster.master.listMsgs());
        assertEquals(List.of(), cluster.secondary(1).listMsgs());
        assertEquals(List.of(), cluster.secondary(2).listMsgs());
        awaitEventually(() -> cluster.secondary(2).listMsgs().equals(List.of("m1")), EVENTUALLY);
    }

    @Test
    void w2WaitsForTheFastestSecondaryOnly() {
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), sec(5));
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(2).id(), sec(9));

        assertBlocksAbout(sec(5), () -> cluster.master.appendMsg("m1", 2));
    }

    @Test
    void w3WaitsForTheSlowestSecondary() {
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), sec(5));
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(2).id(), sec(6));

        assertBlocksAbout(sec(6), () -> cluster.master.appendMsg("m1", 3));
    }

    @Test
    void writeConcernOutOfRangeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> cluster.master.appendMsg("m1", 0));
        assertThrows(IllegalArgumentException.class, () -> cluster.master.appendMsg("m1", 4));
        assertEquals(List.of(), cluster.master.listMsgs());
    }

    @Test
    void messagesAreOrderedWhenTheNetworkReordersThem() {
        var s1 = cluster.secondary(1);
        List<String> arrivalOrder = new CopyOnWriteArrayList<>();
        AtomicInteger sent = countMessagesOnTheLink(s1);
        cluster.transport.reorderMessages(cluster.masterId, s1.id(), 2);
        // rules run in the order they were added, so this one sees the already reordered stream
        cluster.transport.addRule(cluster.masterId, s1.id(), (message, next) -> {
            arrivalOrder.add(message.payload());
            next.run();
        });

        cluster.master.appendMsg("m1", 1);
        // replication runs on another thread: make sure m1 is on the link before m2 is sent
        awaitEventually(() -> sent.get() == 1, EVENTUALLY);
        cluster.master.appendMsg("m2", 1);

        awaitEventually(() -> arrivalOrder.size() == 2, EVENTUALLY);
        assertEquals(List.of("m2", "m1"), arrivalOrder, "the harness should have delivered m2 before m1");
        awaitEventually(() -> s1.listMsgs().equals(List.of("m1", "m2")), EVENTUALLY);
        assertEquals(List.of("m1", "m2"), cluster.master.listMsgs());
    }

    @Test
    void secondaryHidesAMessageUntilTheGapBeforeItIsFilled() {
        var secondary = new SecondaryNode(new NodeId("s"));

        secondary.onReplicate(new Message("id3", 3, "m3"));
        secondary.onReplicate(new Message("id1", 1, "m1"));
        assertEquals(List.of("m1"), secondary.listMsgs());

        secondary.onReplicate(new Message("id2", 2, "m2"));
        assertEquals(List.of("m1", "m2", "m3"), secondary.listMsgs());
    }

    @Test
    void secondaryIgnoresDuplicates() {
        var secondary = new SecondaryNode(new NodeId("s"));
        secondary.onReplicate(new Message("id1", 1, "m1"));
        secondary.onReplicate(new Message("id1", 1, "m1"));
        secondary.onReplicate(new Message("id3", 3, "m3"));
        secondary.onReplicate(new Message("id3", 3, "m3"));
        secondary.onReplicate(new Message("id2", 2, "m2"));

        assertEquals(List.of("m1", "m2", "m3"), secondary.listMsgs());
    }

    @Test
    void totalOrderIsTheSameOnAllNodesUnderConcurrentClients() {
        Random random = new Random(42);
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), Duration.ofMillis(40));
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(2).id(), Duration.ofMillis(10));

        List<CompletableFuture<Void>> clients = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            clients.add(cluster.appendAsync("m" + i, 1 + random.nextInt(3)));
        }
        CompletableFuture.allOf(clients.toArray(CompletableFuture[]::new)).join();

        List<String> masterLog = cluster.master.listMsgs();
        assertEquals(30, masterLog.size());
        awaitEventually(() -> cluster.secondary(1).listMsgs().size() == 30
                && cluster.secondary(2).listMsgs().size() == 30, EVENTUALLY);
        assertEquals(masterLog, cluster.secondary(1).listMsgs());
        assertEquals(masterLog, cluster.secondary(2).listMsgs());
    }

    @Test
    void reorderWithWriteConcernAboveOneStillGivesCorrectOrder() {
        var s1 = cluster.secondary(1);
        AtomicInteger sent = countMessagesOnTheLink(s1);
        cluster.transport.reorderMessages(cluster.masterId, s1.id(), 2);

        // w=2 waits for S1, whose two messages are held until both have been sent, hence the async clients
        CompletableFuture<Void> m1 = cluster.appendAsync("m1", 2);
        awaitEventually(() -> sent.get() == 1, EVENTUALLY);
        CompletableFuture<Void> m2 = cluster.appendAsync("m2", 2);
        CompletableFuture.allOf(m1, m2).join();

        // w=2 may be satisfied by S2 alone, so S1 (the reordered link) is only eventually complete
        awaitEventually(() -> s1.listMsgs().equals(List.of("m1", "m2")), EVENTUALLY);
        awaitEventually(() -> cluster.secondary(2).listMsgs().equals(List.of("m1", "m2")), EVENTUALLY);
    }

    @Test
    void incompleteReorderGroupIsFlushedAfterTheTimeout() {
        var s1 = cluster.secondary(1);
        cluster.transport.reorderMessages(cluster.masterId, s1.id(), 3, Duration.ofMillis(300));

        cluster.master.appendMsg("m1", 1);
        cluster.master.appendMsg("m2", 1);

        // only 2 of 3 messages arrived: they are released anyway, and still listed in order
        awaitEventually(() -> s1.listMsgs().equals(List.of("m1", "m2")), EVENTUALLY);
    }

    @Test
    void failedReplicationMakesAnUnreachableWriteConcernFailInsteadOfHanging() {
        // S2 is not registered in the transport, so it can never ACK
        cluster.transport.unregister(cluster.secondary(2).id());

        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("m1", 2)); // S1 still suffices
        assertThrows(TransportException.class, () -> cluster.master.appendMsg("m2", 3));
        assertEquals(List.of("m1", "m2"), cluster.master.listMsgs());
    }

    /** Counts messages that entered the master -> {@code target} link; add it before the fault rules. */
    private AtomicInteger countMessagesOnTheLink(SecondaryNode target) {
        AtomicInteger count = new AtomicInteger();
        cluster.transport.addRule(cluster.masterId, target.id(), (message, next) -> {
            count.incrementAndGet();
            next.run();
        });
        return count;
    }
}
