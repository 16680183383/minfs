package com.ksyun.campus.client.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private List<MetaServerMsg> slaveMetaServer;
    private List<DataServerMsg> dataServer;
    private Map<String, Object> replicaDistribution;
    private Map<String, Object> healthStatus;

    public MetaServerMsg getMasterMetaServer() {
        return masterMetaServer;
    }

    public void setMasterMetaServer(MetaServerMsg masterMetaServer) {
        this.masterMetaServer = masterMetaServer;
    }

    public List<MetaServerMsg> getSlaveMetaServer() {
        return slaveMetaServer;
    }

    public void setSlaveMetaServer(List<MetaServerMsg> slaveMetaServer) {
        this.slaveMetaServer = slaveMetaServer;
    }

    public List<DataServerMsg> getDataServer() {
        return dataServer;
    }

    public void setDataServer(List<DataServerMsg> dataServer) {
        this.dataServer = dataServer;
    }

    public Map<String, Object> getReplicaDistribution() {
        return replicaDistribution;
    }

    public void setReplicaDistribution(Map<String, Object> replicaDistribution) {
        this.replicaDistribution = replicaDistribution;
    }

    public Map<String, Object> getHealthStatus() {
        return healthStatus;
    }

    public void setHealthStatus(Map<String, Object> healthStatus) {
        this.healthStatus = healthStatus;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +
                ", slaveMetaServer=" + slaveMetaServer +
                ", dataServer=" + dataServer +
                ", replicaDistribution=" + replicaDistribution +
                ", healthStatus=" + healthStatus +
                '}';
    }
}
