package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
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
    // 记录每个 DataServer 节点上次的原始数据（字符串），用于判断是否真正变化
    private final Map<String, String> dataServerLastData = new ConcurrentHashMap<>();
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
     * 监听单个DataServer节点的数据变化
     */
    private void watchDataServerData(String dataServerPath, String serverId) {
        try {
            zooKeeper.getData(dataServerPath + "/" + serverId, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        try {
                            // 重新加载该DataServer的数据
                            byte[] data = zooKeeper.getData(dataServerPath + "/" + serverId, false, null);
                            String serverInfo = data != null ? new String(data) : "";
                            // 与上次原始数据比较，完全一致则不打印日志
                            String prev = dataServerLastData.get(serverId);
                            boolean changed = !java.util.Objects.equals(prev, serverInfo);
                            Map<String, Object> server = parseServerInfo(serverId, serverInfo);
                            dataServers.put(serverId, server);
                            // 更新缓存
                            dataServerLastData.put(serverId, serverInfo);
                            if (changed) {
                                log.info("DataServer {} 数据更新: {}", serverId, serverInfo);
                            } else {
                                log.debug("DataServer {} 数据内容未变，跳过日志", serverId);
                            }
                            
                            // 重新设置监听
                            watchDataServerData(dataServerPath, serverId);
                        } catch (Exception e) {
                            log.error("重新加载DataServer {} 数据失败", serverId, e);
                        }
                    }
                }
            }, null);
        } catch (Exception e) {
            log.error("设置DataServer {} 数据变化监听失败", serverId, e);
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
                    server.put("status", "ACTIVE");
                    server.put("lastHeartbeat", System.currentTimeMillis());
                    log.debug("DataServer {} 心跳正常", serverId);
                } else {
                    if (Boolean.TRUE.equals(server.get("active"))) {
                        log.warn("数据服务器 {} 不在ZK列表中，标记为不可用", serverId);
                    }
                    server.put("active", false);
                    server.put("status", "INACTIVE");
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
                    // 初始化原始数据缓存，避免首次加载后立即触发相同内容的重复日志
                    dataServerLastData.put(child, serverInfo);
                    log.info("加载数据服务器: {} -> {}", child, serverInfo.isEmpty() ? "(no data)" : serverInfo);
                    
                    // 为每个DataServer设置数据变化监听
                    watchDataServerData(dataServerPath, child);
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
            log.debug("解析DataServer信息: serverId={}, info={}", serverId, info);
            
            if (info.startsWith("{")) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> json = objectMapper.readValue(info, Map.class);
                    log.debug("JSON解析成功: {}", json);
                    
                    String ip = String.valueOf(json.getOrDefault("ip", ""));
                    Object portObj = json.get("port");
                    int port = portObj instanceof Number ? ((Number) portObj).intValue() : Integer.parseInt(String.valueOf(portObj));
                    String zone = json.get("zone") != null ? String.valueOf(json.get("zone")).trim() : null;
                    String rack = json.get("rack") != null ? String.valueOf(json.get("rack")) : null;
                    long total = json.get("totalCapacity") instanceof Number ? ((Number) json.get("totalCapacity")).longValue() : 0L;
                    long used = json.get("usedCapacity") instanceof Number ? ((Number) json.get("usedCapacity")).longValue() : 0L;
                    long fileTotal = json.get("fileTotal") instanceof Number ? ((Number) json.get("fileTotal")).longValue() : 0L;
                    String status = json.get("status") != null ? String.valueOf(json.get("status")) : "unknown";

                    log.debug("解析容量信息: totalCapacity={} ({}), usedCapacity={} ({}), 原始值: totalCapacity={}, usedCapacity={}", 
                             total, total, used, used, json.get("totalCapacity"), json.get("usedCapacity"));

                    server.put("ip", ip);
                    server.put("host", ip);
                    server.put("port", port);
                    server.put("address", ip + ":" + port);
                    server.put("zone", zone);
                    server.put("rack", rack);
                    server.put("totalCapacity", total);
                    server.put("usedCapacity", used);
                    server.put("fileTotal", fileTotal);
                    // 计算剩余容量
                    long remainingCapacity = Math.max(0, total - used);
                    server.put("remainingCapacity", remainingCapacity);
                    server.put("active", "alive".equalsIgnoreCase(status));
                    // 添加status字段以保持兼容性
                    server.put("status", status);
                    
                    log.debug("JSON解析后设置字段: totalCapacity={}, usedCapacity={}, remainingCapacity={}", 
                             server.get("totalCapacity"), server.get("usedCapacity"), server.get("remainingCapacity"));
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
                long defaultCapacity = 1024 * 1024 * 1024L; // 1GB
                server.put("totalCapacity", defaultCapacity);
                server.put("usedCapacity", 0L);
                server.put("remainingCapacity", defaultCapacity);
            } else {
                server.put("address", serverId);
                long defaultCapacity = 1024 * 1024 * 1024L; // 1GB
                server.put("totalCapacity", defaultCapacity);
                server.put("usedCapacity", 0L);
                server.put("remainingCapacity", defaultCapacity);
            }
        }
        
        // 确保所有必要的字段都存在，但不覆盖已存在的值
        log.debug("字段检查前: totalCapacity={}, usedCapacity={}, remainingCapacity={}", 
                 server.get("totalCapacity"), server.get("usedCapacity"), server.get("remainingCapacity"));
        
        // 如果totalCapacity和usedCapacity都存在，重新计算remainingCapacity
        if (server.containsKey("totalCapacity") && server.containsKey("usedCapacity")) {
            Object totalObj = server.get("totalCapacity");
            Object usedObj = server.get("usedCapacity");
            if (totalObj instanceof Number && usedObj instanceof Number) {
                long total = ((Number) totalObj).longValue();
                long used = ((Number) usedObj).longValue();
                long remainingCapacity = Math.max(0, total - used);
                server.put("remainingCapacity", remainingCapacity);
                log.debug("重新计算remainingCapacity: {} - {} = {}", total, used, remainingCapacity);
            }
        }
        
        if (!server.containsKey("totalCapacity")) {
            server.put("totalCapacity", 1024 * 1024 * 1024L);
        }
        if (!server.containsKey("usedCapacity")) {
            server.put("usedCapacity", 0L);
        }
        if (!server.containsKey("remainingCapacity")) {
            server.put("remainingCapacity", 0L);
        }
        if (!server.containsKey("active")) {
            server.put("active", true);
        }
        if (!server.containsKey("status")) {
            server.put("status", "ACTIVE");
        }
        server.put("lastHeartbeat", System.currentTimeMillis());
        
        // 添加调试日志
        log.debug("DataServer {} 解析完成: totalCapacity={}, usedCapacity={}, remainingCapacity={}, active={}", 
                 serverId, server.get("totalCapacity"), server.get("usedCapacity"), 
                 server.get("remainingCapacity"), server.get("active"));
        
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
            long capacity = 1024 * 1024 * 1024L; // 1GB 默认值
            if (parts.length >= 3) {
                try {
                    capacity = Long.parseLong(parts[2]);
                } catch (NumberFormatException e) {
                    capacity = 1024 * 1024 * 1024L;
                }
            }
            server.put("totalCapacity", capacity);
            server.put("usedCapacity", 0L);
            server.put("remainingCapacity", capacity);
            server.put("active", true);
            server.put("status", "ACTIVE");
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
