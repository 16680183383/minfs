package com.ksyun.campus.client.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataServerMsg{
    private String host;
    private int port;
    private long fileTotal;
    private long capacity;
    private long useCapacity;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getFileTotal() {
        return fileTotal;
    }

    public void setFileTotal(long fileTotal) {
        this.fileTotal = fileTotal;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getUseCapacity() {
        return useCapacity;
    }

    public void setUseCapacity(long useCapacity) {
        this.useCapacity = useCapacity;
    }

    @Override
    public String toString() {
        return "DataServerMsg{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", fileTotal=" + fileTotal +
                ", capacity=" + capacity +
                ", useCapacity=" + useCapacity +
                '}';
    }
}
