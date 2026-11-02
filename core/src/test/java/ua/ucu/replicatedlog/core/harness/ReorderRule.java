package ua.ucu.replicatedlog.core.harness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ua.ucu.replicatedlog.core.Message;
import ua.ucu.replicatedlog.core.TransportException;

/**
 * Collects messages on the link in groups of {@code count}; once a group is full it is released in reverse
 * order of arrival. Delivery calls are blocking, so every held message stays blocked until its group is
 * complete, and then they are handed to the target one after another in the reordered sequence.
 * <p>
 * If the group does not fill up within {@code flushAfter}, the messages collected so far are released
 * (reversed) anyway, so a test with too few messages does not hang forever.
 */
public class ReorderRule implements LinkRule {

    private static final Duration DEFAULT_FLUSH_AFTER = Duration.ofSeconds(30);

    private final int count;
    private final Duration flushAfter;
    private Group current;

    public ReorderRule(int count) {
        this(count, DEFAULT_FLUSH_AFTER);
    }

    public ReorderRule(int count, Duration flushAfter) {
        if (count < 2) {
            throw new IllegalArgumentException("messagesCount must be at least 2");
        }
        this.count = count;
        this.flushAfter = flushAfter;
        this.current = new Group();
    }

    @Override
    public void apply(Message message, Delivery next) {
        Group group;
        int index;
        synchronized (this) {
            group = current;
            index = group.size++;
            if (group.size == count) {
                current = new Group();
                group.release();
            }
        }
        if (!awaitQuietly(group.released)) {
            synchronized (this) {
                if (current == group) {
                    current = new Group();
                }
                group.release();
            }
        }
        // delivery order = reverse arrival order: the last one to arrive goes first
        int position = group.size - 1 - index;
        await(group.turn(position));
        try {
            next.run();
        } finally {
            group.done(position);
        }
    }

    private boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(flushAfter.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException("Interrupted while reordering", e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException("Interrupted while reordering", e);
        }
    }

    private final class Group {
        int size;
        final CountDownLatch released = new CountDownLatch(1);
        // turns[p] opens when the delivery at position p-1 has finished
        private final List<CountDownLatch> turns = new ArrayList<>();

        Group() {
            for (int i = 0; i < count; i++) {
                turns.add(new CountDownLatch(i == 0 ? 0 : 1));
            }
        }

        void release() {
            released.countDown();
        }

        CountDownLatch turn(int position) {
            return turns.get(position);
        }

        void done(int position) {
            if (position + 1 < turns.size()) {
                turns.get(position + 1).countDown();
            }
        }
    }
}
