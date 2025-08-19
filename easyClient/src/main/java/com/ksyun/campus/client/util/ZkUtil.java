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
     * 获取MetaServer地址列表
     */
    public List<String> getMetaServerAddresses() throws Exception {
        if (zooKeeper == null) {
            throw new IllegalStateException("Zookeeper未连接");
        }
        
        List<String> addresses = new ArrayList<>();
        try {
            List<String> children = zooKeeper.getChildren("/metaservers", false);
            for (String child : children) {
                byte[] data = zooKeeper.getData("/metaservers/" + child, false, null);
                if (data != null) {
                    String address = new String(data);
                    addresses.add(address);
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
            List<String> children = zooKeeper.getChildren("/dataservers", false);
            for (String child : children) {
                byte[] data = zooKeeper.getData("/dataservers/" + child, false, null);
                if (data != null) {
                    String address = new String(data);
                    addresses.add(address);
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
