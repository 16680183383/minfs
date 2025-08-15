package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.FileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class FsckServices {
    
    @Autowired
    private MetaService metaService;
    
    @Autowired
    private ZkDataServerService zkDataServerService;
    
    @Autowired
    private DataServerClientService dataServerClient;
    
    @Autowired
    private MetadataStorageService metadataStorage;
    
    // 存储FSCK检查结果
    private final Map<String, Object> fsckResults = new ConcurrentHashMap<>();
    
    /**
     * 定时执行FSCK检查（每120秒）
     */
    @Scheduled(fixedRate = 120000)
    public void scheduledFsck() {
        log.info("开始定时FSCK检查");
        // 默认检查所有文件系统
        List<String> fileSystems = metadataStorage.getFileSystemList();
        if (fileSystems.isEmpty()) {
            // 如果没有文件系统，使用默认名称
            performFsck("default");
        } else {
            // 检查所有文件系统
            for (String fileSystemName : fileSystems) {
                performFsck(fileSystemName);
            }
        }
    }
    
    /**
     * 手动触发FSCK检查
     */
    public void manualFsck(String fileSystemName) {
        log.info("开始手动FSCK检查: fileSystemName={}", fileSystemName);
        performFsck(fileSystemName);
    }
    
    /**
     * 执行FSCK检查
     */
    private void performFsck(String fileSystemName) {
        long startTime = System.currentTimeMillis();
        Map<String, Object> results = new HashMap<>();
        
        try {
            // 检查所有文件
            List<StatInfo> allFiles = metaService.getAllFiles(fileSystemName);
            int totalFiles = allFiles.size();
            int corruptedFiles = 0;
            int repairedFiles = 0;
            
            log.info("开始检查文件系统 {} 中的 {} 个文件", fileSystemName, totalFiles);
            
            for (StatInfo statInfo : allFiles) {
                if (statInfo.getType() == FileType.File) {
                    if (checkFileReplicas(statInfo)) {
                        corruptedFiles++;
                        if (repairFileReplicas(fileSystemName, statInfo)) {
                            repairedFiles++;
                        }
                    }
                }
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            results.put("fileSystemName", fileSystemName);
            results.put("totalFiles", totalFiles);
            results.put("corruptedFiles", corruptedFiles);
            results.put("repairedFiles", repairedFiles);
            results.put("checkDuration", duration + "ms");
            results.put("status", "completed");
            results.put("timestamp", new Date());
            
            log.info("文件系统 {} FSCK检查完成: 总文件数={}, 损坏文件数={}, 修复文件数={}, 耗时={}ms", 
                    fileSystemName, totalFiles, corruptedFiles, repairedFiles, duration);
            
        } catch (Exception e) {
            log.error("文件系统 {} FSCK检查异常", fileSystemName, e);
            results.put("fileSystemName", fileSystemName);
            results.put("status", "error");
            results.put("error", e.getMessage());
            results.put("timestamp", new Date());
        }
        
        // 更新结果
        fsckResults.clear();
        fsckResults.putAll(results);
    }
    
    /**
     * 检查文件副本状态
     */
    private boolean checkFileReplicas(StatInfo statInfo) {
        List<ReplicaData> replicas = statInfo.getReplicaData();
        if (replicas == null || replicas.size() < 3) {
            log.warn("文件 {} 副本数量不足: {}", statInfo.getPath(), 
                    replicas != null ? replicas.size() : 0);
            return true; // 需要修复
        }
        
        // 检查每个副本的可用性
        int availableReplicas = 0;
        for (ReplicaData replica : replicas) {
            if (dataServerClient.isDataServerAvailable(replica.dsNode)) {
                availableReplicas++;
            } else {
                log.warn("文件 {} 的副本 {} 不可用: {}", 
                        statInfo.getPath(), replica.id, replica.dsNode);
            }
        }
        
        if (availableReplicas < 2) {
            log.warn("文件 {} 可用副本数量不足: {}/{}", 
                    statInfo.getPath(), availableReplicas, replicas.size());
            return true; // 需要修复
        }
        
        return false; // 不需要修复
    }
    
    /**
     * 修复文件副本
     */
    private boolean repairFileReplicas(String fileSystemName, StatInfo statInfo) {
        log.info("开始修复文件系统 {} 中文件 {} 的副本", fileSystemName, statInfo.getPath());
        
        List<ReplicaData> replicas = statInfo.getReplicaData();
        List<ReplicaData> newReplicas = new ArrayList<>();
        
        // 保留可用的副本
        for (ReplicaData replica : replicas) {
            if (dataServerClient.isDataServerAvailable(replica.dsNode)) {
                newReplicas.add(replica);
            }
        }
        
        // 如果可用副本不足3个，创建新副本
        while (newReplicas.size() < 3) {
            ReplicaData newReplica = createNewReplica(fileSystemName, statInfo.getPath(), statInfo.getSize());
            if (newReplica != null) {
                newReplicas.add(newReplica);
                log.info("为文件系统 {} 中文件 {} 创建新副本: {}", fileSystemName, statInfo.getPath(), newReplica.dsNode);
            } else {
                log.error("无法为文件系统 {} 中文件 {} 创建新副本", fileSystemName, statInfo.getPath());
                break;
            }
        }
        
        if (newReplicas.size() >= 3) {
            statInfo.setReplicaData(newReplicas);
            metaService.updateFileSize(fileSystemName, statInfo.getPath(), statInfo.getSize()); // 持久化更改
            log.info("成功修复文件系统 {} 中文件 {} 的副本", fileSystemName, statInfo.getPath());
            return true;
        } else {
            log.error("修复文件系统 {} 中文件 {} 副本失败，副本数量: {}", fileSystemName, statInfo.getPath(), newReplicas.size());
            return false;
        }
    }
    
    /**
     * 创建新副本
     */
    private ReplicaData createNewReplica(String fileSystemName, String filePath, long fileSize) {
        try {
            // 获取可用的DataServer
            List<Map<String, Object>> availableServers = zkDataServerService.getActiveDataServers();
            
            // 选择副本数量最少的服务器
            Map<String, Integer> replicaCounts = metaService.getReplicaDistribution(fileSystemName);
            availableServers.sort((a, b) -> {
                String idA = (String) a.get("id");
                String idB = (String) b.get("id");
                int countA = replicaCounts.getOrDefault(idA, 0);
                int countB = replicaCounts.getOrDefault(idB, 0);
                return Integer.compare(countA, countB);
            });
            
            if (!availableServers.isEmpty()) {
                Map<String, Object> selectedServer = availableServers.get(0);
                
                ReplicaData replica = new ReplicaData();
                replica.id = UUID.randomUUID().toString();
                replica.dsNode = (String) selectedServer.get("address");
                replica.path = "/data/" + selectedServer.get("id") + "/" + fileSystemName + "/" + filePath.replace("/", "_");
                replica.offset = 0;
                replica.length = fileSize;
                replica.isPrimary = false; // 新副本不是主副本
                
                return replica;
            }
            
        } catch (Exception e) {
            log.error("创建新副本异常", e);
        }
        
        return null;
    }
    
    /**
     * 获取FSCK检查结果
     */
    public Map<String, Object> getFsckResults(String fileSystemName) {
        Map<String, Object> results = new HashMap<>(fsckResults);
        // 确保返回结果包含正确的文件系统名称
        results.put("fileSystemName", fileSystemName);
        return results;
    }
    
    /**
     * 获取服务状态
     */
    public Map<String, Object> getServiceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("fsckEnabled", true);
        status.put("lastCheckTime", fsckResults.get("timestamp"));
        status.put("lastCheckStatus", fsckResults.get("status"));
        status.put("dataServerStatus", dataServerClient.getDataServerStatus());
        return status;
    }
}

