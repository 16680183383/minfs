package com.example.dataserver.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class RegistService implements ApplicationRunner {

    // 从配置文件获取当前dataServer的属性
    @Value("${dataserver.ip}")
    private String ip;
    @Value("${server.port}")
    private int port;
    @Value("${dataserver.total-capacity}") // 总容量（MB）
    private long totalCapacity;
    @Value("${dataserver.rack}") // 机架标识
    private String rack;
    @Value("${dataserver.zone}") // 可用区标识
    private String zone;
    @Value("${zk.address}")
    private String zkAddress; // zk地址，默认值需符合文档要求无需外部传入
    @Value("${dataserver.storage.path}")
    private String localStoragePath; // 本地存储根目录，从配置文件获取

    private CuratorFramework zkClient;
    private final String ZK_BASE_PATH = "/minfs/dataservers"; // zk注册根路径
    private String selfNodePath; // 本实例在zk的节点路径

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // 初始化zk客户端
        zkClient = CuratorFrameworkFactory.newClient(
                zkAddress,
                new ExponentialBackoffRetry(1000, 3)
        );
        zkClient.start();
        // 注册服务并启动状态上报
        registToCenter();
        startStatusReport();
    }

    public void registToCenter() {
        // todo 将本实例信息注册至zk中心，包含信息 ip、port、capacity、rack、zone
        try {
            // 创建zk根路径（若不存在）
            if (zkClient.checkExists().forPath(ZK_BASE_PATH) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(ZK_BASE_PATH);
            }
            // 本实例节点路径（临时节点，服务下线后自动删除，用于心跳检测）
            selfNodePath = ZK_BASE_PATH + "/" + ip + ":" + port;
            // 节点数据：存储ip、port、容量、rack、zone等信息
            Map<String, Object> nodeData = new HashMap<>();
            nodeData.put("ip", ip);
            nodeData.put("port", port);
            nodeData.put("totalCapacity", totalCapacity);
            nodeData.put("usedCapacity", 0); // 初始已用容量为0
            nodeData.put("rack", rack);
            nodeData.put("zone", zone);
            nodeData.put("status", "alive");
            // 序列化数据并创建临时节点（临时节点特性用于故障检测）
            zkClient.create()
                    .withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                    .forPath(selfNodePath, new ObjectMapper().writeValueAsBytes(nodeData));
        } catch (Exception e) {
            throw new RuntimeException("Failed to register dataServer to zk", e);
        }
    }

    /**
     * 定期上报状态，符合文档中"定期上报中心zk，本实例实际使用情况、容量等"的要求
     */
    private void startStatusReport() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        // 每20s上报一次（小于文档要求的30s心跳超时）
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 更新已用容量（实际应计算本地存储的文件总大小）
                long usedCapacity = calculateUsedCapacity();
                // 读取当前节点数据并更新
                byte[] data = zkClient.getData().forPath(selfNodePath);
                Map<String, Object> nodeData = new ObjectMapper().readValue(data, Map.class);
                nodeData.put("usedCapacity", usedCapacity);
                nodeData.put("status", "alive");
                // 写入更新后的数据
                zkClient.setData().forPath(selfNodePath, new ObjectMapper().writeValueAsBytes(nodeData));
            } catch (Exception e) {
                System.err.println("Failed to report status to zk: " + e.getMessage());
            }
        }, 0, 20, TimeUnit.SECONDS);
    }

    /**
     * 获取所有可用dataServer列表（从zk查询）
     */
    public List<Map<String, Object>> getDslist() {
        try {
            List<String> children = zkClient.getChildren().forPath(ZK_BASE_PATH);
            List<Map<String, Object>> dsList = new ArrayList<>();
            for (String child : children) {
                String nodePath = ZK_BASE_PATH + "/" + child;
                byte[] data = zkClient.getData().forPath(nodePath);
                Map<String, Object> dsInfo = new ObjectMapper().readValue(data, Map.class);
                dsList.add(dsInfo);
            }
            return dsList;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get dataServer list from zk", e);
        }
    }

    /**
     * 计算本地已用容量（实际实现需遍历本地存储目录）
     */
    /**
     * 计算本地存储目录下所有文件的总大小（单位：MB）
     * 遍历存储路径下的所有文件和子目录，累加文件大小
     */
    private long calculateUsedCapacity() {
        // 从配置文件获取本地存储根目录（对应 dataserver.storage.path）
        String storagePath = localStoragePath; // 需要在类中添加此属性并注入，参考下方说明
        File rootDir = new File(storagePath);

        // 若目录不存在，已用容量为0
        if (!rootDir.exists() || !rootDir.isDirectory()) {
            return 0;
        }

        // 递归计算目录总大小（单位：字节）
        long totalBytes = calculateDirectorySize(rootDir);

        // 转换为MB（1MB = 1024 * 1024字节）
        return totalBytes / (1024 * 1024);
    }

    /**
     * 递归计算目录下所有文件的总大小（字节）
     */
    private long calculateDirectorySize(File directory) {
        long size = 0;

        // 获取目录下的所有文件和子目录
        File[] files = directory.listFiles();
        if (files == null) { // 目录为空或无权限
            return 0;
        }

        for (File file : files) {
            if (file.isFile()) {
                // 累加文件大小
                size += file.length();
            } else if (file.isDirectory()) {
                // 递归计算子目录大小
                size += calculateDirectorySize(file);
            }
        }
        return size;
    }

}
