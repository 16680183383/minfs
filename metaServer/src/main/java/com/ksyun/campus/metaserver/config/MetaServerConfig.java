package com.ksyun.campus.metaserver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "metaserver")
public class MetaServerConfig {
    
    private String host;
    private ZkConfig zk;
    private ClusterConfig cluster;
    
    @Data
    public static class ZkConfig {
        private String connectString;
        private String rootPath;
    }
    
    @Data
    public static class ClusterConfig {
        private String role;
        private ElectionConfig election;
        private HeartbeatConfig heartbeat;
        private List<NodeConfig> nodes;
    }
    
    @Data
    public static class ElectionConfig {
        private boolean enabled;
    }
    
    @Data
    public static class HeartbeatConfig {
        private long interval;
        private long timeout;
    }
    
    @Data
    public static class NodeConfig {
        private String host;
        private int port;
        private String role;
    }
}
