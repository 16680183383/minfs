package com.example.dataserver.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class ZkService {
    
    @Value("${metaserver.zk.connect.string:localhost:2181}")
    private String zkConnectString;
    
    @Value("${metaserver.zk.root.path:/minfs}")
    private String zkRootPath;
    
    @Value("${dataserver.ip:localhost}")
    private String serverIp;
    
    @Value("${server.port:9000}")
    private int serverPort;
    
    @Value("${dataserver.storage.path:D:/data/apps/minfs/dataserver}")
    private String storagePath;
    
    private ZooKeeper zooKeeper;
    private String dataServerNodePath;
    private final AtomicLong usedCapacity = new AtomicLong(0);
    private final AtomicLong totalCapacity = new AtomicLong(1024 * 1024 * 1024L); // 1GB 默认容量
    
    @PostConstruct
    public void init() {
        try {
            connectToZookeeper();
            registerDataServer();
            startHeartbeat();
            log.info("DataServer ZK服务初始化完成");
        } catch (Exception e) {
            log.error("初始化ZK服务失败", e);
        }
    }
    
    private void connectToZookeeper() throws Exception {
        CountDownLatch connectedSignal = new CountDownLatch(1);
        
        zooKeeper = new ZooKeeper(zkConnectString, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                } else if (event.getState() == Event.KeeperState.Disconnected) {
                    log.warn("ZK连接断开");
                } else if (event.getState() == Event.KeeperState.Expired) {
                    log.error("ZK会话过期");
                }
            }
        });
        
        connectedSignal.await();
        log.info("成功连接到Zookeeper: {}", zkConnectString);
    }
    
    private void registerDataServer() {
        try {
            // 使用 host:port 格式作为节点ID，与MetaServer期望的格式一致
            String dataServerId = serverIp + ":" + serverPort;
            dataServerNodePath = zkRootPath + "/dataservers/" + dataServerId;
            
            // 创建dataservers路径
            try {
                zooKeeper.create(zkRootPath + "/dataservers", new byte[0], 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("创建Zookeeper数据服务器路径: {}", zkRootPath + "/dataservers");
            } catch (KeeperException.NodeExistsException e) {
                log.debug("Zookeeper数据服务器路径已存在: {}", zkRootPath + "/dataservers");
            }
            
            // 如果节点已存在，先删除再创建
            try {
                zooKeeper.delete(dataServerNodePath, -1);
                log.info("删除已存在的DataServer节点: {}", dataServerNodePath);
            } catch (KeeperException.NoNodeException e) {
                log.debug("DataServer节点不存在，无需删除: {}", dataServerNodePath);
            } catch (Exception e) {
                log.warn("删除已存在的DataServer节点失败: {}", e.getMessage());
            }
            
            // 计算实际存储容量
            calculateTotalCapacity();
            
            // 注册DataServer节点
            String serverInfo = createServerInfo();
            zooKeeper.create(dataServerNodePath, serverInfo.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            
            log.info("DataServer注册成功: {} -> {}", dataServerNodePath, serverInfo);
            
        } catch (Exception e) {
            log.error("注册DataServer失败", e);
        }
    }
    
    private void calculateTotalCapacity() {
        try {
            java.io.File storageDir = new java.io.File(storagePath);
            if (storageDir.exists() && storageDir.isDirectory()) {
                // 使用总空间作为总容量
                long totalSpace = storageDir.getTotalSpace();
                totalCapacity.set(totalSpace);
                log.info("计算存储容量: 总容量={} 字节", totalSpace);
            } else {
                // 如果目录不存在，创建目录并设置默认容量
                if (storageDir.mkdirs()) {
                    log.info("创建存储目录: {}", storagePath);
                }
                // 使用配置文件中的容量值，如果没有则使用默认值
                long configCapacity = 1024 * 1024 * 1024L; // 1GB 默认值
                totalCapacity.set(configCapacity);
                log.info("使用配置存储容量: {} 字节", configCapacity);
            }
            
            // 计算当前已使用的容量
            long currentUsedCapacity = calculateCurrentUsedCapacity();
            usedCapacity.set(currentUsedCapacity);
            log.info("计算当前已用容量: {} 字节", currentUsedCapacity);
            
        } catch (Exception e) {
            log.warn("计算存储容量失败，使用默认值", e);
            totalCapacity.set(1024 * 1024 * 1024L); // 1GB
            usedCapacity.set(0L);
        }
    }
    
    /**
     * 计算当前已使用的容量
     */
    private long calculateCurrentUsedCapacity() {
        try {
            java.io.File storageDir = new java.io.File(storagePath);
            if (!storageDir.exists() || !storageDir.isDirectory()) {
                return 0L;
            }
            
            return calculateDirectorySize(storageDir);
        } catch (Exception e) {
            log.warn("计算已用容量失败", e);
            return 0L;
        }
    }
    
    /**
     * 递归计算目录大小
     */
    private long calculateDirectorySize(java.io.File directory) {
        long size = 0L;
        
        try {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    if (file.isFile()) {
                        size += file.length();
                    } else if (file.isDirectory()) {
                        size += calculateDirectorySize(file);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("计算目录大小失败: {}", directory.getAbsolutePath(), e);
        }
        
        return size;
    }
    
    private String createServerInfo() {
        return String.format(
            "{\"ip\":\"%s\",\"port\":%d,\"totalCapacity\":%d,\"usedCapacity\":%d,\"status\":\"alive\"}",
            serverIp, serverPort, totalCapacity.get(), usedCapacity.get()
        );
    }
    
    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    updateHeartbeat();
                    Thread.sleep(10000); // 每10秒更新一次心跳
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("心跳更新失败", e);
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.setName("DataServer-Heartbeat");
        heartbeatThread.start();
    }
    
    private void updateHeartbeat() {
        try {
            if (zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                // 重新计算当前已用容量
                long currentUsedCapacity = calculateCurrentUsedCapacity();
                usedCapacity.set(currentUsedCapacity);
                
                String serverInfo = createServerInfo();
                zooKeeper.setData(dataServerNodePath, serverInfo.getBytes(), -1);
                log.debug("心跳更新成功: totalCapacity={}, usedCapacity={}", 
                         totalCapacity.get(), usedCapacity.get());
            }
        } catch (Exception e) {
            log.warn("心跳更新失败", e);
        }
    }
    
    /**
     * 更新已使用容量
     */
    public void updateUsedCapacity(long newUsedCapacity) {
        usedCapacity.set(newUsedCapacity);
        log.info("更新已使用容量: {} 字节", newUsedCapacity);
        
        // 立即更新ZK节点
        try {
            if (zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                String serverInfo = createServerInfo();
                zooKeeper.setData(dataServerNodePath, serverInfo.getBytes(), -1);
                log.debug("ZK节点容量更新成功: usedCapacity={}", newUsedCapacity);
            }
        } catch (Exception e) {
            log.warn("ZK节点容量更新失败", e);
        }
    }
    
    /**
     * 增加已使用容量
     */
    public void addUsedCapacity(long additionalCapacity) {
        long newCapacity = usedCapacity.addAndGet(additionalCapacity);
        log.info("增加已使用容量: +{} 字节, 总计: {} 字节", additionalCapacity, newCapacity);
        updateUsedCapacity(newCapacity);
    }
    
    /**
     * 减少已使用容量
     */
    public void subtractUsedCapacity(long reducedCapacity) {
        long newCapacity = usedCapacity.addAndGet(-reducedCapacity);
        if (newCapacity < 0) {
            newCapacity = 0;
            usedCapacity.set(0);
        }
        log.info("减少已使用容量: -{} 字节, 总计: {} 字节", reducedCapacity, newCapacity);
        updateUsedCapacity(newCapacity);
    }
    
    /**
     * 获取当前已使用容量
     */
    public long getUsedCapacity() {
        return usedCapacity.get();
    }
    
    /**
     * 获取总容量
     */
    public long getTotalCapacity() {
        return totalCapacity.get();
    }
    
    @PreDestroy
    public void destroy() {
        try {
            // 删除ZK节点
            if (zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED && dataServerNodePath != null) {
                try {
                    zooKeeper.delete(dataServerNodePath, -1);
                    log.info("DataServer ZK节点删除成功: {}", dataServerNodePath);
                } catch (KeeperException.NoNodeException e) {
                    log.debug("DataServer ZK节点已不存在: {}", dataServerNodePath);
                } catch (Exception e) {
                    log.warn("删除DataServer ZK节点失败", e);
                }
            }
            
            // 关闭ZK连接
            if (zooKeeper != null) {
                zooKeeper.close();
                log.info("ZK连接已关闭");
            }
        } catch (Exception e) {
            log.error("销毁ZK服务失败", e);
        }
    }
}
