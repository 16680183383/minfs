package com.ksyun.campus.metaserver.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
public class RegistService implements ApplicationRunner {
    
    @Value("${server.port:8000}")
    private int serverPort;
    
    @Value("${metaserver.zk.connect.string:localhost:2181}")
    private String zkConnectString;
    
    @Value("${metaserver.zk.root.path:/minfs}")
    private String zkRootPath;
    
    private ZooKeeper zooKeeper;
    private String nodePath;
    
    public void registToCenter(){
        try {
            // 连接Zookeeper
            connectToZookeeper();
            
            // 创建根路径
            createRootPath();
            
            // 获取本机IP地址
            String localIp = InetAddress.getLocalHost().getHostAddress();
            String serverInfo = localIp + ":" + serverPort;
            
            // 注册到Zookeeper
            registerToZookeeper(serverInfo);
            
            log.info("MetaServer注册成功: {} -> {}", serverInfo, nodePath);
            
        } catch (Exception e) {
            log.error("MetaServer注册失败", e);
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
        log.info("成功连接到Zookeeper: {}", zkConnectString);
    }
    
    private void createRootPath() throws Exception {
        String[] paths = zkRootPath.split("/");
        StringBuilder currentPath = new StringBuilder();
        
        for (String path : paths) {
            if (!path.isEmpty()) {
                currentPath.append("/").append(path);
                try {
                    zooKeeper.create(currentPath.toString(), new byte[0], 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    log.debug("创建Zookeeper路径: {}", currentPath.toString());
                } catch (KeeperException.NodeExistsException e) {
                    // 路径已存在，忽略
                    log.debug("Zookeeper路径已存在: {}", currentPath.toString());
                }
            }
        }
    }
    
    private void registerToZookeeper(String serverInfo) throws Exception {
        String metaServerPath = zkRootPath + "/metaservers";
        
        // 创建metaservers路径
        try {
            zooKeeper.create(metaServerPath, new byte[0], 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // 路径已存在，忽略
        }
        
        // 注册当前实例
        nodePath = zooKeeper.create(metaServerPath + "/meta-", 
            serverInfo.getBytes(), 
            ZooDefs.Ids.OPEN_ACL_UNSAFE, 
            CreateMode.EPHEMERAL_SEQUENTIAL);
        
        log.info("MetaServer节点创建成功: {}", nodePath);
    }
    
    public void unregisterFromCenter() {
        if (zooKeeper != null && nodePath != null) {
            try {
                zooKeeper.delete(nodePath, -1);
                log.info("MetaServer注销成功: {}", nodePath);
            } catch (Exception e) {
                log.error("MetaServer注销失败", e);
            }
        }
    }
    
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    public String getNodePath() {
        return nodePath;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }
}
