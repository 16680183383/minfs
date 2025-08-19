package com.ksyun.campus.client.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReplicaData {
    public String id;
    public String dsNode;//格式为ip:port
    public String path;
    public int offset;
    public int length;
    public boolean isPrimary;

    @Override
    public String toString() {
        return "ReplicaData{" +
                "id='" + id + '\'' +
                ", dsNode='" + dsNode + '\'' +
                ", path='" + path + '\'' +
                ", offset=" + offset +
                ", length=" + length +
                ", isPrimary=" + isPrimary +
                '}';
    }
}
