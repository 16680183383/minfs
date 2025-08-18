package com.example.easyclient.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


/**
 * 适配文档架构：zk用于识别metaServer master、监听dataServer信息
 * 功能：初始化zk连接、监听服务节点、缓存最新metaServer/dataServer地址
 */
public class ZkUtil {
    private static final Logger LOG = (Logger) LoggerFactory.getLogger(ZkUtil.class);
    // zk默认地址（文档未指定，按部署规范预设，可配置）
    private String zkConnectString = "127.0.0.1:2181";
    // zk会话超时（<30s，符合文档心跳超时要求）
    private int sessionTimeout = 20000;
    private ZooKeeper zkClient;
    // 缓存：最新master metaServer地址（host:port）
    private volatile String masterMetaAddr;
    // 缓存：所有slave metaServer地址列表
    private volatile List<String> slaveMetaAddrs = new ArrayList<>();
    // 缓存：所有在线dataServer地址列表（host:port）
    private volatile List<String> dataServerAddrs = new ArrayList<>();

    // zk节点路径（按minFS架构约定，可与服务端对齐）
    private static final String META_SERVER_ROOT = "/minfs/metaServers";
    private static final String DATA_SERVER_ROOT = "/minfs/dataServers";
    private static final String MASTER_NODE = META_SERVER_ROOT + "/master";


    @PostConstruct
    public void postCons() throws Exception {
        // 1. 初始化zk连接（阻塞等待连接建立）
        CountDownLatch connectLatch = new CountDownLatch(1);
        zkClient = new ZooKeeper(zkConnectString, sessionTimeout, event -> {
            if (Watcher.Event.KeeperState.SyncConnected == event.getState()) {
                connectLatch.countDown();
                LOG.info("zk连接成功");
            }
        });
        connectLatch.await();

        // 2. 检查并创建zk根节点（初始化系统必要信息，符合文档1-19启动脚本要求）
        checkAndCreateNode(META_SERVER_ROOT, CreateMode.PERSISTENT);
        checkAndCreateNode(DATA_SERVER_ROOT, CreateMode.PERSISTENT);

        // 3. 监听master metaServer（适配B8：metaServer主从切换感知）
        watchMasterMetaServer();
        // 4. 监听slave metaServer列表
        watchSlaveMetaServers();
        // 5. 监听dataServer列表（适配B9：dataServer状态感知）
        watchDataServers();
    }


    /**
     * 监听master metaServer节点（主从切换时更新缓存）
     */
    private void watchMasterMetaServer() throws KeeperException, InterruptedException {
        // 读取master节点数据（存储格式：host:port）
        byte[] data = zkClient.getData(MASTER_NODE, event -> {
            if (Watcher.Event.EventType.NodeDataChanged == event.getType()) {
                LOG.info("master metaServer节点变化，重新监听");
                try {
                    watchMasterMetaServer(); // 重新监听（zk一次性监听）
                } catch (Exception e) {
                    LOG.error("重新监听master失败", e);
                }
            }
        }, new Stat());

        // 更新master地址缓存
        if (data != null && data.length > 0) {
            this.masterMetaAddr = new String(data);
            LOG.info("更新master metaServer地址：{}", masterMetaAddr);
        }
    }


    /**
     * 监听slave metaServer子节点（增减slave时更新缓存）
     */
    private void watchSlaveMetaServers() throws KeeperException, InterruptedException {
        // 读取slave子节点列表（子节点名格式：slave-1、slave-2，数据为host:port）
        List<String> slaveNodes = zkClient.getChildren(META_SERVER_ROOT, event -> {
            if (Watcher.Event.EventType.NodeChildrenChanged == event.getType()) {
                LOG.info("slave metaServer节点列表变化，重新监听");
                try {
                    watchSlaveMetaServers();
                } catch (Exception e) {
                    LOG.error("重新监听slave metaServer失败", e);
                }
            }
        });

        // 遍历子节点，排除master，读取slave地址
        List<String> newSlaveAddrs = new ArrayList<>();
        for (String node : slaveNodes) {
            if (!MASTER_NODE.equals(META_SERVER_ROOT + "/" + node)) {
                byte[] data = zkClient.getData(META_SERVER_ROOT + "/" + node, false, new Stat());
                newSlaveAddrs.add(new String(data));
            }
        }
        this.slaveMetaAddrs = newSlaveAddrs;
        LOG.info("更新slave metaServer地址列表：{}", slaveMetaAddrs);
    }


    /**
     * 监听dataServer子节点（增减dataServer时更新缓存）
     */
    private void watchDataServers() throws KeeperException, InterruptedException {
        // 读取dataServer子节点列表（子节点名格式：ds-1，数据为host:port）
        List<String> dsNodes = zkClient.getChildren(DATA_SERVER_ROOT, event -> {
            if (Watcher.Event.EventType.NodeChildrenChanged == event.getType()) {
                LOG.info("dataServer节点列表变化，重新监听");
                try {
                    watchDataServers();
                } catch (Exception e) {
                    LOG.error("重新监听dataServer失败", e);
                }
            }
        });

        // 读取所有在线dataServer地址
        List<String> newDsAddrs = new ArrayList<>();
        for (String node : dsNodes) {
            byte[] data = zkClient.getData(DATA_SERVER_ROOT + "/" + node, false, new Stat());
            newDsAddrs.add(new String(data));
        }
        this.dataServerAddrs = newDsAddrs;
        LOG.info("更新dataServer地址列表：{}", dataServerAddrs);
    }


    /**
     * 检查zk节点是否存在，不存在则创建
     */
    private void checkAndCreateNode(String path, CreateMode mode) throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            zkClient.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
            LOG.info("创建zk节点：{}", path);
        }
    }


    // -------------------------- 对外提供缓存的服务地址 --------------------------
    public String getMasterMetaAddr() {
        if (masterMetaAddr == null || masterMetaAddr.isEmpty()) {
            throw new RuntimeException("未获取到master metaServer地址，请检查zk服务");
        }
        return masterMetaAddr;
    }

    public List<String> getSlaveMetaAddrs() {
        return slaveMetaAddrs;
    }

    public List<String> getDataServerAddrs() {
        return dataServerAddrs;
    }

    public void close() throws InterruptedException {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}