package ua.ucu.replicatedlog.core.harness;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** Assertion helpers for timing- and eventual-consistency checks, reusable in every iteration's tests. */
public final class Timing {

    private Timing() {
    }

    public static Duration measure(Runnable action) {
        long start = System.nanoTime();
        action.run();
        return Duration.ofNanos(System.nanoTime() - start);
    }

    /** The call must block for at least {@code min} but not much longer ({@link TestTime#TOLERANCE}). */
    public static void assertBlocksAbout(Duration expected, Runnable action) {
        Duration elapsed = measure(action);
        assertTrue(elapsed.compareTo(expected) >= 0,
                "expected to block at least " + expected.toMillis() + " ms but took " + elapsed.toMillis() + " ms");
        Duration max = expected.plus(TestTime.TOLERANCE);
        assertTrue(elapsed.compareTo(max) < 0,
                "expected to finish within " + max.toMillis() + " ms but took " + elapsed.toMillis() + " ms");
    }

    public static void assertCompletesWithin(Duration max, Runnable action) {
        Duration elapsed = measure(action);
        assertTrue(elapsed.compareTo(max) < 0,
                "expected not to block (< " + max.toMillis() + " ms) but took " + elapsed.toMillis() + " ms");
    }

    /** Polls until the condition holds or the timeout elapses (for eventual-consistency checks). */
    public static void awaitEventually(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "condition not met within " + timeout.toMillis() + " ms");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
