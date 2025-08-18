package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class ZkMetaServerService implements ApplicationRunner {
    
    @Value("${metaserver.zk.connect.string:localhost:2181}")
    private String zkConnectString;
    
    @Value("${metaserver.zk.root.path:/minfs}")
    private String zkRootPath;
    
    @Value("${server.port:8000}")
    private int serverPort;
    
    @Value("${metaserver.host:localhost}")
    private String serverHost;
    
    private ZooKeeper zooKeeper;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private String leaderPath;
    private String metaServerPath;
    
    @PostConstruct
    public void init() {
        try {
            connectToZookeeper();
            createMetaServerNode();
            startLeaderElection();
            log.info("ZkMetaServerService初始化完成");
        } catch (Exception e) {
            log.error("初始化ZK元数据服务失败", e);
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
                    reconnectToZookeeper();
                }
            }
        });
        
        connectedSignal.await();
        log.info("成功连接到Zookeeper: {}", zkConnectString);
    }
    
    private void reconnectToZookeeper() {
        log.info("尝试重新连接Zookeeper...");
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
            connectToZookeeper();
            createMetaServerNode();
            startLeaderElection();
            log.info("重新连接Zookeeper成功");
        } catch (Exception e) {
            log.error("重新连接Zookeeper失败", e);
        }
    }
    
    private void createMetaServerNode() {
        try {
            String metaServerId = "meta" + serverPort;
            metaServerPath = zkRootPath + "/metaservers/" + metaServerId;
            
            // 创建metaservers路径
            try {
                zooKeeper.create(zkRootPath + "/metaservers", new byte[0], 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("创建Zookeeper元数据服务器路径: {}", zkRootPath + "/metaservers");
            } catch (KeeperException.NodeExistsException e) {
                log.debug("Zookeeper元数据服务器路径已存在: {}", zkRootPath + "/metaservers");
            }
            
            // 创建当前MetaServer节点
            String serverInfo = serverHost + ":" + serverPort + ":active:" + System.currentTimeMillis();
            zooKeeper.create(metaServerPath, serverInfo.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.info("创建MetaServer节点: {} -> {}", metaServerPath, serverInfo);
            
        } catch (Exception e) {
            log.error("创建MetaServer节点失败", e);
        }
    }
    
    private void startLeaderElection() {
        try {
            leaderPath = zkRootPath + "/leader";
            
            // 创建leader路径
            try {
                zooKeeper.create(leaderPath, new byte[0], 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("创建Zookeeper leader路径: {}", leaderPath);
            } catch (KeeperException.NodeExistsException e) {
                log.debug("Zookeeper leader路径已存在: {}", leaderPath);
            }
            
            // 尝试成为leader
            tryBecomeLeader();
            
        } catch (Exception e) {
            log.error("启动leader选举失败", e);
        }
    }
    
    private void tryBecomeLeader() {
        try {
            // 修复: 使用固定的leader节点名称，而不是每个MetaServer都有自己的leader节点
            String leaderNodePath = leaderPath + "/leader";
            String leaderData = serverHost + ":" + serverPort + ":" + System.currentTimeMillis();
            
            zooKeeper.create(leaderNodePath, leaderData.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            
            // 成功创建leader节点，成为leader
            isLeader.set(true);
            log.info("成功成为leader: {} -> {}", leaderNodePath, leaderData);
            
            // 设置leader节点删除监听
            watchLeaderNode(leaderNodePath);
            
        } catch (KeeperException.NodeExistsException e) {
            // leader节点已存在，成为follower
            log.info("leader节点已存在，成为follower");
            watchLeaderNode();
        } catch (Exception e) {
            log.error("尝试成为leader失败", e);
        }
    }
    
    private void watchLeaderNode() {
        try {
            // 监听leader路径下的子节点变化
            zooKeeper.getChildren(leaderPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        log.info("检测到leader节点变化，重新尝试成为leader");
                        // 重新尝试成为leader
                        tryBecomeLeader();
                    }
                }
            });
        } catch (Exception e) {
            log.error("设置leader节点监听失败", e);
        }
    }
    
    private void watchLeaderNode(String leaderNodePath) {
        try {
            // 监听当前leader节点
            zooKeeper.exists(leaderNodePath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        log.warn("Leader节点被删除，重新启动leader选举");
                        isLeader.set(false);
                        tryBecomeLeader();
                    }
                }
            });
        } catch (Exception e) {
            log.error("设置leader节点监听失败", e);
        }
    }
    
    /**
     * 获取所有MetaServer节点信息
     */
    public List<Map<String, Object>> getAllMetaServers() {
        List<Map<String, Object>> metaServers = new ArrayList<>();
        try {
            String metaServerPath = zkRootPath + "/metaservers";
            List<String> children = zooKeeper.getChildren(metaServerPath, false);
            
            for (String child : children) {
                try {
                    String fullPath = metaServerPath + "/" + child;
                    byte[] data = zooKeeper.getData(fullPath, false, null);
                    String serverInfo = new String(data);
                    
                    Map<String, Object> server = new HashMap<>();
                    server.put("id", child);
                    server.put("path", fullPath);
                    server.put("info", serverInfo);
                    
                    // 解析服务器信息
                    String[] parts = serverInfo.split(":");
                    if (parts.length >= 3) {
                        server.put("host", parts[0]);
                        server.put("port", Integer.parseInt(parts[1]));
                        server.put("status", parts[2]);
                        server.put("address", parts[0] + ":" + parts[1]);
                        if (parts.length >= 4) {
                            server.put("registerTime", Long.parseLong(parts[3]));
                        }
                    }
                    
                    metaServers.add(server);
                } catch (Exception e) {
                    log.warn("获取MetaServer节点信息失败: {}", child, e);
                }
            }
        } catch (Exception e) {
            log.error("获取所有MetaServer节点失败", e);
        }
        return metaServers;
    }
    
    /**
     * 获取当前MetaServer信息
     */
    public Map<String, Object> getCurrentMetaServerInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("host", serverHost);
        info.put("port", serverPort);
        info.put("path", metaServerPath);
        info.put("isLeader", isLeader.get());
        info.put("zkConnected", zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED);
        return info;
    }
    
    /**
     * 检查是否为Leader
     */
    public boolean isLeader() {
        return isLeader.get();
    }

    /**
     * 获取当前Leader地址 host:port
     */
    public String getLeaderAddress() {
        try {
            String leaderNodePath = zkRootPath + "/leader/leader";
            byte[] data = zooKeeper.getData(leaderNodePath, false, null);
            if (data != null && data.length > 0) {
                String s = new String(data);
                String[] parts = s.split(":");
                if (parts.length >= 2) {
                    return parts[0] + ":" + parts[1];
                }
            }
        } catch (Exception e) {
            log.warn("获取Leader地址失败", e);
        }
        return null;
    }
    
    /**
     * 获取ZK连接状态
     */
    public boolean isZkConnected() {
        return zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED;
    }
    
    /**
     * 获取ZK实例
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    /**
     * 获取当前节点路径
     */
    public String getMetaServerPath() {
        return metaServerPath;
    }
    
    @PreDestroy
    public void destroy() {
        isRunning.set(false);
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
                log.info("ZkMetaServerService销毁完成");
            }
        } catch (Exception e) {
            log.error("销毁ZkMetaServerService失败", e);
        }
    }
    
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("ZkMetaServerService启动完成，当前节点: {}", metaServerPath);
    }
}
