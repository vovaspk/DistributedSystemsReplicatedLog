package ua.ucu.replicatedlog.sec;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ua.ucu.replicatedlog.core.NodeId;
import ua.ucu.replicatedlog.core.SecondaryNode;

@Configuration
public class SecondaryConfig {

    @Bean
    SecondaryNode secondaryNode(@Value("${sec.node-id:secondary}") String nodeId,
                                @Value("${sec.delay-ms:0}") long delayMs) {
        return new SecondaryNode(new NodeId(nodeId), Duration.ofMillis(delayMs));
    }
}
