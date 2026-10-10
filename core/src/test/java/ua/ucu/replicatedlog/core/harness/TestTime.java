package ua.ucu.replicatedlog.core.harness;

import java.time.Duration;

/**
 * Central place for test timing. Scenarios are written with the assignment's numbers (5 s, 6 s, ...) and
 * scaled down by {@link #SCALE} so the suite stays fast. If timing ever becomes flaky, change it here only.
 */
public final class TestTime {

    /** The assignment's seconds are divided by this factor (5 s becomes 500 ms). */
    public static final int SCALE = 10;

    /** Allowed overshoot when asserting "takes about X". */
    public static final Duration TOLERANCE = Duration.ofMillis(250);

    private TestTime() {
    }

    /** {@code sec(5)} is "5 seconds" in assignment terms, i.e. 500 ms of real time. */
    public static Duration sec(long assignmentSeconds) {
        return Duration.ofSeconds(assignmentSeconds).dividedBy(SCALE);
    }
}
