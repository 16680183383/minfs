package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String dataServerPath;
    
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
            this.dataServerPath = dataServerPath;
            
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
        try {
            if (dataServerPath == null) {
                dataServerPath = zkRootPath + "/dataservers";
            }
            List<String> children = zooKeeper.getChildren(dataServerPath, false);
            Set<String> aliveSet = new HashSet<>(children);

            // 标记存在的为active并刷新时间，不存在的为inactive
            for (Map.Entry<String, Map<String, Object>> entry : dataServers.entrySet()) {
                String serverId = entry.getKey();
                Map<String, Object> server = entry.getValue();
                if (aliveSet.contains(serverId)) {
                    server.put("active", true);
                    server.put("lastHeartbeat", System.currentTimeMillis());
                } else {
                    if (Boolean.TRUE.equals(server.get("active"))) {
                        log.warn("数据服务器 {} 不在ZK列表中，标记为不可用", serverId);
                    }
                    server.put("active", false);
                }
            }
        } catch (Exception e) {
            log.error("心跳存在性检查失败", e);
        }
    }
    
    private void loadExistingDataServers(String dataServerPath) {
        try {
            List<String> children = zooKeeper.getChildren(dataServerPath, false);
            dataServers.clear();
            
            for (String child : children) {
                try {
                    byte[] data = zooKeeper.getData(dataServerPath + "/" + child, false, null);
                    String serverInfo = data != null ? new String(data) : "";
                    Map<String, Object> server = parseServerInfo(child, serverInfo);
                    dataServers.put(child, server);
                    log.info("加载数据服务器: {} -> {}", child, serverInfo.isEmpty() ? "(no data)" : serverInfo);
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
        
        // 优先解析节点data；支持JSON格式：
        // {"rack":"rack-04","port":9003,"zone":"az-01 ","totalCapacity":102400,"usedCapacity":0,"ip":"localhost","status":"alive"}
        if (serverInfo != null && !serverInfo.isEmpty()) {
            String info = serverInfo.trim();
            if (info.startsWith("{")) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> json = objectMapper.readValue(info, Map.class);
                    String ip = String.valueOf(json.getOrDefault("ip", ""));
                    Object portObj = json.get("port");
                    int port = portObj instanceof Number ? ((Number) portObj).intValue() : Integer.parseInt(String.valueOf(portObj));
                    String zone = json.get("zone") != null ? String.valueOf(json.get("zone")).trim() : null;
                    String rack = json.get("rack") != null ? String.valueOf(json.get("rack")) : null;
                    long total = json.get("totalCapacity") instanceof Number ? ((Number) json.get("totalCapacity")).longValue() : 0L;
                    long used = json.get("usedCapacity") instanceof Number ? ((Number) json.get("usedCapacity")).longValue() : 0L;
                    String status = json.get("status") != null ? String.valueOf(json.get("status")) : "unknown";

                    server.put("ip", ip);
                    server.put("host", ip);
                    server.put("port", port);
                    server.put("address", ip + ":" + port);
                    server.put("zone", zone);
                    server.put("rack", rack);
                    server.put("totalCapacity", total);
                    server.put("usedCapacity", used);
                    server.put("capacity", total);
                    server.put("usedSpace", used);
                    server.put("active", "alive".equalsIgnoreCase(status));
                } catch (Exception jsonEx) {
                    log.warn("解析DataServer JSON失败，尝试按host:port:capacity解析: {}", info, jsonEx);
                    fillFromColonSeparated(server, info);
                }
            } else {
                fillFromColonSeparated(server, info);
            }
        } else {
            // 从节点名解析
            if (serverId.contains(":")) {
                String[] hp = serverId.split(":", 2);
                server.put("host", hp[0]);
                try {
                    server.put("port", Integer.parseInt(hp[1]));
                } catch (NumberFormatException ignore) {
                    server.put("port", 0);
                }
                server.put("address", serverId);
                server.put("capacity", 1024 * 1024 * 1024L);
            } else {
                server.put("address", serverId);
            }
        }
        
        server.put("usedSpace", 0L);
        server.putIfAbsent("active", true);
        server.put("lastHeartbeat", System.currentTimeMillis());
        
        return server;
    }

    private void fillFromColonSeparated(Map<String, Object> server, String info) {
        String[] parts = info.split(":");
        if (parts.length >= 2) {
            server.put("host", parts[0]);
            try {
                server.put("port", Integer.parseInt(parts[1]));
            } catch (NumberFormatException e) {
                server.put("port", 0);
            }
            server.put("address", parts[0] + ":" + parts[1]);
            if (parts.length >= 3) {
                try {
                    server.put("capacity", Long.parseLong(parts[2]));
                } catch (NumberFormatException e) {
                    server.put("capacity", 1024 * 1024 * 1024L);
                }
            } else {
                server.put("capacity", 1024 * 1024 * 1024L);
            }
            server.put("active", true);
        }
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
