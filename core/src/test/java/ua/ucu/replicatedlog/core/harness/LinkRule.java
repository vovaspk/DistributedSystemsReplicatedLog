package ua.ucu.replicatedlog.core.harness;

import ua.ucu.replicatedlog.core.Message;

/**
 * A fault injected on one directed link of the {@link MockedTransport}. A rule wraps the delivery: it may
 * delay it, fail before it, fail after it, hold the message back and release it reordered, etc. by deciding
 * when (and whether) to call {@code next}.
 * <p>
 * Iteration 1 only needs {@link DelayRule}; reorder (Iteration 2) and drop / failure rules (Iteration 3)
 * are new implementations of this interface, no changes to the transport or the tests are needed.
 */
@FunctionalInterface
public interface LinkRule {

    void apply(Message message, Delivery next);

    @FunctionalInterface
    interface Delivery {
        void run();
    }
}
