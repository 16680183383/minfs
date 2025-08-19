package com.ksyun.campus.client.util;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Zookeeper工具类
 * 提供Zookeeper连接和操作功能
 */
public class ZkUtil {
    
    private ZooKeeper zooKeeper;
    private String connectString = "localhost:2181";
    private int sessionTimeout = 30000;
    private CountDownLatch connectionLatch = new CountDownLatch(1);

    // 统一ZK根路径，便于读取leader
    private String zkRootPath = "/minfs";
    
    /**
     * 连接到Zookeeper
     */
    public void connectToZookeeper() throws Exception {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });
        
        // 等待连接建立
        connectionLatch.await();
    }
    
    /**
     * 获取当前Leader的地址（host:port）
     * 读取路径: /minfs/leader/leader，内容形如 host:port:timestamp
     */
    public String getLeaderAddress() throws Exception {
        if (zooKeeper == null) {
            throw new IllegalStateException("Zookeeper未连接");
        }
        try {
            String path = zkRootPath + "/leader/leader";
            byte[] data = zooKeeper.getData(path, false, (Stat) null);
            if (data == null || data.length == 0) {
                return null;
            }
            String raw = new String(data);
            // 可能为 host:port 或 host:port:timestamp
            String[] parts = raw.split(":");
            if (parts.length >= 2) {
                return parts[0] + ":" + parts[1];
            }
            return raw;
        } catch (Exception e) {
            // 路径不存在或其他异常则返回null，交由调用方回退
            return null;
        }
    }
    
    /**
     * 获取MetaServer地址列表
     */
    public List<String> getMetaServerAddresses() throws Exception {
        if (zooKeeper == null) {
            throw new IllegalStateException("Zookeeper未连接");
        }
        List<String> addresses = new ArrayList<>();
        try {
            String path = zkRootPath + "/metaservers";
            List<String> children = zooKeeper.getChildren(path, false);
            for (String child : children) {
                byte[] data = zooKeeper.getData(path + "/" + child, false, null);
                if (data != null && data.length > 0) {
                    addresses.add(new String(data));
                } else {
                    // 回退：有些实现可能把地址放在节点名
                    addresses.add(child);
                }
            }
        } catch (Exception e) {
            // 如果路径不存在，返回空列表
        }
        return addresses;
    }
    
    /**
     * 获取DataServer地址列表
     */
    public List<String> getDataServerAddresses() throws Exception {
        if (zooKeeper == null) {
            throw new IllegalStateException("Zookeeper未连接");
        }
        List<String> addresses = new ArrayList<>();
        try {
            String path = zkRootPath + "/dataservers";
            List<String> children = zooKeeper.getChildren(path, false);
            for (String child : children) {
                byte[] data = zooKeeper.getData(path + "/" + child, false, null);
                if (data != null && data.length > 0) {
                    addresses.add(new String(data));
                } else {
                    addresses.add(child);
                }
            }
        } catch (Exception e) {
            // 如果路径不存在，返回空列表
        }
        return addresses;
    }
    
    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED;
    }
    
    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (Exception e) {
            // 忽略关闭时的异常
        }
    }
    
    /**
     * 设置连接字符串
     */
    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }
    
    /**
     * 设置会话超时时间
     */
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }
}
