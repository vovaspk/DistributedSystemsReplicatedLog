package ua.ucu.replicatedlog.core.harness;

import java.time.Duration;

import ua.ucu.replicatedlog.core.Message;
import ua.ucu.replicatedlog.core.TransportException;

/** Adds latency to every message on the link before it reaches the target node. */
public record DelayRule(Duration delay) implements LinkRule {

    @Override
    public void apply(Message message, Delivery next) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException("Interrupted while delaying message", e);
        }
        next.run();
    }
}
