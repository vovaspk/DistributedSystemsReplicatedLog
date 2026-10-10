package ua.ucu.replicatedlog.master;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ua.ucu.replicatedlog.core.MasterNode;
import ua.ucu.replicatedlog.core.NodeId;
import ua.ucu.replicatedlog.core.Transport;

@Configuration
public class MasterConfig {

    @Bean
    Transport transport() {
        return new HttpTransport();
    }

    @Bean
    MasterNode masterNode(Transport transport, @Value("${replication.secondaries:}") List<String> secondaries) {
        List<NodeId> secondaryIds = secondaries.stream()
                .filter(s -> !s.isBlank())
                .map(NodeId::new)
                .toList();
        return new MasterNode(new NodeId("master"), transport, secondaryIds);
    }
}
