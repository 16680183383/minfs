package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
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
    private MetadataStorageService metadataStorage;
    
    // 存储FSCK检查结果（全局）
    private final Map<String, Object> fsckResults = new ConcurrentHashMap<>();
    
    /**
     * 定时执行FSCK检查（每120秒）
     */
    @Scheduled(fixedRate = 120000)
    public void scheduledFsck() {
        log.info("开始定时FSCK检查");
        performFsck();
    }
    
    /**
     * 手动触发FSCK检查
     */
    public void manualFsck() {
        log.info("开始手动FSCK检查");
        performFsck();
    }
    
    /**
     * 执行FSCK检查
     */
    private void performFsck() {
        long startTime = System.currentTimeMillis();
        Map<String, Object> results = new HashMap<>();
        
        try {
            // 检查所有文件
            List<StatInfo> allFiles = metaService.getAllFiles();
            int totalFiles = allFiles.size();
            int orphanedFiles = 0;
            
            log.info("开始检查所有文件，共 {} 个", totalFiles);
            
            // 检查孤儿文件（元数据存在但副本信息不完整）
            for (StatInfo statInfo : allFiles) {
                if (statInfo.getType() == FileType.File) {
                    if (checkOrphanedFile(statInfo)) {
                        orphanedFiles++;
                    }
                }
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            results.put("totalFiles", totalFiles);
            results.put("orphanedFiles", orphanedFiles);
            results.put("checkDuration", duration + "ms");
            results.put("status", "completed");
            results.put("timestamp", new Date());
            
            log.info("FSCK检查完成: 总文件数={}, 孤儿文件数={}, 耗时={}ms", 
                    totalFiles, orphanedFiles, duration);
            
        } catch (Exception e) {
            log.error("FSCK检查异常", e);
            results.put("status", "error");
            results.put("error", e.getMessage());
            results.put("timestamp", new Date());
        }
        
        // 保存检查结果
        fsckResults.put("global", results);
    }
    
    /**
     * 检查孤儿文件（元数据存在但副本信息不完整）
     */
    private boolean checkOrphanedFile(StatInfo statInfo) {
        if (statInfo.getReplicaData() == null || statInfo.getReplicaData().isEmpty()) {
            log.warn("发现孤儿文件: {}, 缺少副本信息", statInfo.getPath());
            return true;
        }
        
        // 检查副本数量是否足够
        if (statInfo.getReplicaData().size() < 3) {
            log.warn("发现副本不足的文件: {}, 副本数量: {}", 
                    statInfo.getPath(), statInfo.getReplicaData().size());
            return true;
        }
        
        return false;
    }
    
    /**
     * 获取FSCK检查结果
     */
    public Map<String, Object> getFsckResults() {
        return (Map<String, Object>) fsckResults.getOrDefault("global", new HashMap<>());
    }
    
    /**
     * 获取所有FSCK检查结果
     */
    public Map<String, Object> getAllFsckResults() {
        return new HashMap<>(fsckResults);
    }
    
    /**
     * 清理FSCK检查结果
     */
    public void clearFsckResults() {
        fsckResults.clear();
    }
}

