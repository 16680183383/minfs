package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class ZkMetaServerService {
    
    @Value("${metaserver.zk.connect.string:localhost:2181}")
    private String zkConnectString;
    
    @Value("${metaserver.zk.root.path:/minfs}")
    private String zkRootPath;
    
    @Value("${server.port:8000}")
    private int serverPort;
    
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
                }
            }
        });
        
        connectedSignal.await();
        log.info("ZK元数据服务成功连接到Zookeeper: {}", zkConnectString);
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
            String serverInfo = "localhost:" + serverPort + ":active";
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
            String leaderNodePath = leaderPath + "/meta" + serverPort;
            String leaderData = "localhost:" + serverPort + ":" + System.currentTimeMillis();
            
            zooKeeper.create(leaderNodePath, leaderData.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            
            // 成功创建leader节点，成为leader
            isLeader.set(true);
            log.info("成功成为leader: {}", leaderNodePath);
            
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
            log.error("设置当前leader节点监听失败", e);
        }
    }
    
    /**
     * 检查当前节点是否是leader
     */
    public boolean isLeader() {
        return isLeader.get();
    }
    
    /**
     * 获取当前leader信息
     */
    public String getCurrentLeader() {
        try {
            List<String> children = zooKeeper.getChildren(leaderPath, false);
            if (!children.isEmpty()) {
                // 返回第一个leader节点
                String leaderId = children.get(0);
                byte[] data = zooKeeper.getData(leaderPath + "/" + leaderId, false, null);
                return new String(data);
            }
        } catch (Exception e) {
            log.error("获取当前leader信息失败", e);
        }
        return null;
    }
    
    /**
     * 获取所有MetaServer节点
     */
    public List<String> getAllMetaServers() {
        try {
            return zooKeeper.getChildren(zkRootPath + "/metaservers", false);
        } catch (Exception e) {
            log.error("获取所有MetaServer节点失败", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 更新MetaServer状态
     */
    public void updateMetaServerStatus(String status) {
        try {
            String serverInfo = "localhost:" + serverPort + ":" + status;
            zooKeeper.setData(metaServerPath, serverInfo.getBytes(), -1);
            log.debug("更新MetaServer状态: {}", status);
        } catch (Exception e) {
            log.error("更新MetaServer状态失败", e);
        }
    }
    
    /**
     * 获取集群状态
     */
    public Map<String, Object> getClusterStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isLeader", isLeader.get());
        status.put("serverPort", serverPort);
        status.put("currentLeader", getCurrentLeader());
        status.put("allMetaServers", getAllMetaServers());
        status.put("zkConnected", zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED);
        return status;
    }
    
    @PreDestroy
    public void close() {
        isRunning.set(false);
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                log.info("ZK元数据服务已关闭");
            } catch (InterruptedException e) {
                log.error("关闭ZK连接失败", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
