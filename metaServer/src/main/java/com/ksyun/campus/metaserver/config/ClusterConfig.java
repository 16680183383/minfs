package com.ksyun.campus.metaserver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "cluster")
public class ClusterConfig {
    
    private MetaServerConfig metaserver;
    private DataServerConfig dataserver;
    
    @Data
    public static class MetaServerConfig {
        private List<NodeConfig> nodes;
    }
    
    @Data
    public static class DataServerConfig {
        private int minReplicas;
        private HeartbeatConfig heartbeat;
    }
    
    @Data
    public static class NodeConfig {
        private String host;
        private int port;
        private String role;
    }
    
    @Data
    public static class HeartbeatConfig {
        private long interval;
        private long timeout;
    }
}
