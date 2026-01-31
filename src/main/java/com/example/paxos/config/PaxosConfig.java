package com.example.paxos.config;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.v1.PaxosNodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;
import java.util.stream.Collectors;

@Configuration
public class PaxosConfig {

    @Value("${paxos.nodeId}")
    private String nodeId;

    //@Value("#{${paxos.peers}}")
    private Map<String, String> peerAddresses; //{n1=localhost:50051, n2=localhost:50052}

    public PaxosConfig() {
         peerAddresses = new HashMap<>();
         this.peerAddresses.put("n1", "localhost:50051");
        this.peerAddresses.put("n2", "localhost:50052");
        this.peerAddresses.put("n3", "localhost:50053");
        this.peerAddresses.put("n4", "localhost:50054");
        this.peerAddresses.put("n5", "localhost:50055");
    }
    @Bean
    public PaxosCore paxosCore() {
        // all nodes (from config)
        Set<String> allNodes = new HashSet<>(peerAddresses.keySet());

        // initialize with default balances A..J = 10
        return new PaxosCore(nodeId, allNodes, null);
    }

    @Bean
    public Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peerStubs() {
        return peerAddresses.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            String[] parts = e.getValue().split(":");
                            ManagedChannel channel = ManagedChannelBuilder
                                    .forAddress(parts[0], Integer.parseInt(parts[1]))
                                    .usePlaintext()
                                    .build();
                            return PaxosNodeGrpc.newBlockingStub(channel);
                        }
                ));
    }

    @Bean
    public String nodeId() {
        return nodeId;
    }
}
