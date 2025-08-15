package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
public class ZkDataServerService {
    
    @Value("${metaserver.zk.connect.string:localhost:2181}")
    private String zkConnectString;
    
    @Value("${metaserver.zk.root.path:/minfs}")
    private String zkRootPath;
    
    private ZooKeeper zooKeeper;
    private final Map<String, Map<String, Object>> dataServers = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void init() {
        try {
            connectToZookeeper();
            watchDataServers();
            startHeartbeatCheck();
        } catch (Exception e) {
            log.error("初始化ZK数据服务器服务失败", e);
        }
    }
    
    private void connectToZookeeper() throws Exception {
        CountDownLatch connectedSignal = new CountDownLatch(1);
        
        zooKeeper = new ZooKeeper(zkConnectString, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        
        connectedSignal.await();
        log.info("ZK数据服务器服务成功连接到Zookeeper: {}", zkConnectString);
    }
    
    private void watchDataServers() {
        try {
            String dataServerPath = zkRootPath + "/dataservers";
            
            // 创建dataservers路径
            try {
                zooKeeper.create(dataServerPath, new byte[0], 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("创建Zookeeper数据服务器路径: {}", dataServerPath);
            } catch (KeeperException.NodeExistsException e) {
                log.debug("Zookeeper数据服务器路径已存在: {}", dataServerPath);
            }
            
            // 监听数据服务器变化
            watchDataServerChanges(dataServerPath);
            
            // 初始加载现有数据服务器
            loadExistingDataServers(dataServerPath);
            
        } catch (Exception e) {
            log.error("监听数据服务器变化失败", e);
        }
    }
    
    private void watchDataServerChanges(String dataServerPath) {
        try {
            zooKeeper.getChildren(dataServerPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        log.info("检测到数据服务器列表变化，重新加载...");
                        loadExistingDataServers(dataServerPath);
                        // 重新设置监听
                        watchDataServerChanges(dataServerPath);
                    }
                }
            });
        } catch (Exception e) {
            log.error("设置数据服务器变化监听失败", e);
        }
    }
    
    /**
     * 启动心跳检测任务
     */
    private void startHeartbeatCheck() {
        Thread heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    checkDataServerHeartbeats();
                    Thread.sleep(10000); // 每10秒检查一次
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("心跳检查失败", e);
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.setName("DataServer-Heartbeat-Checker");
        heartbeatThread.start();
    }
    
    /**
     * 检查数据服务器心跳
     */
    private void checkDataServerHeartbeats() {
        long currentTime = System.currentTimeMillis();
        long timeout = 30000; // 30秒超时
        
        for (Map.Entry<String, Map<String, Object>> entry : dataServers.entrySet()) {
            String serverId = entry.getKey();
            Map<String, Object> server = entry.getValue();
            
            Long lastHeartbeat = (Long) server.get("lastHeartbeat");
            if (lastHeartbeat != null && currentTime - lastHeartbeat > timeout) {
                log.warn("数据服务器 {} 心跳超时，标记为不可用", serverId);
                markDataServerInactive(serverId);
            }
        }
    }
    
    private void loadExistingDataServers(String dataServerPath) {
        try {
            List<String> children = zooKeeper.getChildren(dataServerPath, false);
            dataServers.clear();
            
            for (String child : children) {
                try {
                    byte[] data = zooKeeper.getData(dataServerPath + "/" + child, false, null);
                    String serverInfo = new String(data);
                    Map<String, Object> server = parseServerInfo(child, serverInfo);
                    dataServers.put(child, server);
                    log.info("加载数据服务器: {} -> {}", child, serverInfo);
                } catch (Exception e) {
                    log.error("加载数据服务器 {} 信息失败", child, e);
                }
            }
            
            log.info("成功加载 {} 个数据服务器", dataServers.size());
            
        } catch (Exception e) {
            log.error("加载现有数据服务器失败", e);
        }
    }
    
    private Map<String, Object> parseServerInfo(String serverId, String serverInfo) {
        Map<String, Object> server = new HashMap<>();
        server.put("id", serverId);
        
        // 解析服务器信息格式: "host:port:capacity"
        String[] parts = serverInfo.split(":");
        if (parts.length >= 2) {
            server.put("host", parts[0]);
            server.put("port", Integer.parseInt(parts[1]));
            server.put("address", parts[0] + ":" + parts[1]);
            
            if (parts.length >= 3) {
                server.put("capacity", Long.parseLong(parts[2]));
            } else {
                server.put("capacity", 1024 * 1024 * 1024L); // 默认1GB
            }
        }
        
        server.put("usedSpace", 0L);
        server.put("active", true);
        server.put("lastHeartbeat", System.currentTimeMillis());
        
        return server;
    }
    
    public List<Map<String, Object>> getActiveDataServers() {
        return dataServers.values().stream()
                .filter(server -> (Boolean) server.get("active"))
                .toList();
    }
    
    public Map<String, Map<String, Object>> getAllDataServers() {
        return new HashMap<>(dataServers);
    }
    
    public void updateDataServerHeartbeat(String serverId) {
        Map<String, Object> server = dataServers.get(serverId);
        if (server != null) {
            server.put("lastHeartbeat", System.currentTimeMillis());
            server.put("active", true);
            log.debug("更新数据服务器 {} 心跳", serverId);
        }
    }
    
    public void markDataServerInactive(String serverId) {
        Map<String, Object> server = dataServers.get(serverId);
        if (server != null) {
            server.put("active", false);
            log.warn("标记数据服务器 {} 为不可用", serverId);
        }
    }
    
    public int getActiveDataServerCount() {
        return (int) dataServers.values().stream()
                .filter(server -> (Boolean) server.get("active"))
                .count();
    }
    
    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                log.info("ZK数据服务器服务已关闭");
            } catch (InterruptedException e) {
                log.error("关闭ZK连接失败", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
