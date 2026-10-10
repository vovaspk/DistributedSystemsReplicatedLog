package ua.ucu.replicatedlog.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ua.ucu.replicatedlog.core.harness.TestTime.TOLERANCE;
import static ua.ucu.replicatedlog.core.harness.TestTime.sec;
import static ua.ucu.replicatedlog.core.harness.Timing.assertBlocksAbout;
import static ua.ucu.replicatedlog.core.harness.Timing.assertCompletesWithin;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ua.ucu.replicatedlog.core.harness.TestCluster;

/**
 * Iteration 1: replication is blocking (master waits for ACKs) and parallel. Delays use the assignment's
 * numbers (5 s / 6 s), scaled down by {@code TestTime.SCALE}.
 */
class Iteration1ReplicationTest {

    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = TestCluster.withSecondaries(2);
    }

    @Test
    void appendBlocksUntilTheDelayedSecondaryAcks() {
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), sec(5));

        assertBlocksAbout(sec(5), () -> cluster.master.appendMsg("m1"));

        assertEquals(List.of("m1"), cluster.master.listMsgs());
        // the append returned only after S1 stored the message, i.e. master really waited for its ACK
        assertEquals(List.of("m1"), cluster.secondary(1).listMsgs());
    }

    @Test
    void replicationRunsInParallel() {
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(1).id(), sec(5));
        cluster.transport.setDelay(cluster.masterId, cluster.secondary(2).id(), sec(6));

        // parallel: ~max(5, 6) = 6 s; sequential would take 5 + 6 = 11 s
        assertBlocksAbout(sec(6), () -> cluster.master.appendMsg("m1"));

        assertEquals(List.of("m1"), cluster.secondary(1).listMsgs());
        assertEquals(List.of("m1"), cluster.secondary(2).listMsgs());
    }

    /** The assignment's harness scenario, step by step. */
    @Test
    void assignmentScenario() {
        var s1 = cluster.secondary(1);
        var s2 = cluster.secondary(2);

        cluster.transport.setDelay(cluster.masterId, s1.id(), sec(5));
        assertBlocksAbout(sec(5), () -> cluster.master.appendMsg("m1"));
        assertEquals(List.of("m1"), cluster.master.listMsgs());

        cluster.transport.setDelay(cluster.masterId, s2.id(), sec(6));
        assertBlocksAbout(sec(6), () -> cluster.master.appendMsg("m2"));
        assertEquals(List.of("m1", "m2"), cluster.master.listMsgs());

        cluster.transport.removeDelay(cluster.masterId, s1.id());
        cluster.transport.removeDelay(cluster.masterId, s2.id());
        assertCompletesWithin(TOLERANCE, () -> cluster.master.appendMsg("m3"));

        List<String> all = List.of("m1", "m2", "m3");
        assertEquals(all, cluster.master.listMsgs());
        assertEquals(all, s1.listMsgs());
        assertEquals(all, s2.listMsgs());
    }
}
