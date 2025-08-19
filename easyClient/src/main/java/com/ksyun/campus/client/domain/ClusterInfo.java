package com.ksyun.campus.client.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterInfo {
    private Map<String, Object> metaServers;
    private List<Map<String, Object>> dataServers;
    private Integer totalDataServers;
    private Integer activeDataServers;
    private Map<String, Object> replicaDistribution;
    private Map<String, Object> healthStatus;
    private String error;

    // 兼容旧版本的字段
    private MetaServerMsg masterMetaServer;
    private MetaServerMsg slaveMetaServer;
    private List<DataServerMsg> dataServer;

    // 新的字段
    public Map<String, Object> getMetaServers() {
        return metaServers;
    }

    public void setMetaServers(Map<String, Object> metaServers) {
        this.metaServers = metaServers;
    }

    public List<Map<String, Object>> getDataServers() {
        return dataServers;
    }

    public void setDataServers(List<Map<String, Object>> dataServers) {
        this.dataServers = dataServers;
    }

    public Integer getTotalDataServers() {
        return totalDataServers;
    }

    public void setTotalDataServers(Integer totalDataServers) {
        this.totalDataServers = totalDataServers;
    }

    public Integer getActiveDataServers() {
        return activeDataServers;
    }

    public void setActiveDataServers(Integer activeDataServers) {
        this.activeDataServers = activeDataServers;
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

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    // 兼容旧版本的字段
    public MetaServerMsg getMasterMetaServer() {
        return masterMetaServer;
    }

    public void setMasterMetaServer(MetaServerMsg masterMetaServer) {
        this.masterMetaServer = masterMetaServer;
    }

    public MetaServerMsg getSlaveMetaServer() {
        return slaveMetaServer;
    }

    public void setSlaveMetaServer(MetaServerMsg slaveMetaServer) {
        this.slaveMetaServer = slaveMetaServer;
    }

    public List<DataServerMsg> getDataServer() {
        return dataServer;
    }

    public void setDataServer(List<DataServerMsg> dataServer) {
        this.dataServer = dataServer;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "metaServers=" + metaServers +
                ", dataServers=" + dataServers +
                ", totalDataServers=" + totalDataServers +
                ", activeDataServers=" + activeDataServers +
                ", replicaDistribution=" + replicaDistribution +
                ", healthStatus=" + healthStatus +
                ", error=" + error +
                '}';
    }
}
