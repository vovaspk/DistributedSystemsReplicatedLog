package ua.ucu.replicatedlog.master;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import ua.ucu.replicatedlog.core.Message;
import ua.ucu.replicatedlog.core.NodeId;
import ua.ucu.replicatedlog.core.Transport;
import ua.ucu.replicatedlog.core.TransportException;

/** Real transport: the target {@link NodeId} is the secondary's base URL; a 2xx response is the ACK. */
public class HttpTransport implements Transport {

    private final RestClient restClient = RestClient.create();

    @Override
    public void send(NodeId from, NodeId to, Message message) {
        try {
            restClient.post()
                    .uri(to.value() + "/replicate")
                    .body(message)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException e) {
            throw new TransportException("Replication to " + to + " failed", e);
        }
    }
}
