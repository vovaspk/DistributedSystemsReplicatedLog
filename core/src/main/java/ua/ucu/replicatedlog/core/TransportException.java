package ua.ucu.replicatedlog.core;

/** Thrown by a {@link Transport} when a message could not be delivered or acknowledged. */
public class TransportException extends RuntimeException {

    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
