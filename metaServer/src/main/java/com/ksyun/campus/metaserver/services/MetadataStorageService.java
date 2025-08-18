package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class MetadataStorageService {
    
    private RocksDB rocksDB;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // 从配置文件读取RocksDB路径
    @Value("${metadata.storage.path:./rocksdb_metadata}")
    private String dbPath;
    
    // 缓存全局路径集合（不再区分文件系统）
    private final Set<String> allPaths = ConcurrentHashMap.newKeySet();
    
    @PostConstruct
    public void init() {
        try {
            log.info("初始化RocksDB，路径: {}", dbPath);
            
            File dbDir = new File(dbPath);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
                log.info("创建RocksDB目录: {}", dbPath);
            }
            
            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setMaxBackgroundJobs(4);
            options.setMaxWriteBufferNumber(4);
            options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
            options.setLevel0SlowdownWritesTrigger(8);
            options.setLevel0StopWritesTrigger(12);
            
            rocksDB = RocksDB.open(options, dbPath);
            log.info("RocksDB初始化成功，路径: {}", dbPath);
            
            // 重建文件系统路径缓存
            rebuildFileSystemCache();
            
        } catch (RocksDBException e) {
            log.error("RocksDB初始化失败，路径: {}", dbPath, e);
            throw new RuntimeException("RocksDB初始化失败: " + dbPath, e);
        } catch (Exception e) {
            log.error("RocksDB初始化过程中发生未知错误", e);
            throw new RuntimeException("RocksDB初始化失败", e);
        }
    }
    
    /**
     * 重建文件系统路径缓存
     */
    private void rebuildFileSystemCache() {
        try {
            allPaths.clear();
            RocksIterator iterator = rocksDB.newIterator();
            iterator.seekToFirst();
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                // 现在键即为路径
                allPaths.add(key);
                iterator.next();
            }
            iterator.close();
            
            log.info("路径缓存重建完成，共 {} 条路径", allPaths.size());
        } catch (Exception e) {
            log.error("重建文件系统路径缓存失败", e);
        }
    }
    
    /**
     * 保存元数据
     */
    public void saveMetadata(String path, StatInfo statInfo) {
        try {
            String key = path;
            String json = objectMapper.writeValueAsString(statInfo);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            
            rocksDB.put(key.getBytes(StandardCharsets.UTF_8), value);
            
            // 更新缓存
            allPaths.add(path);
            
            log.debug("保存元数据成功: {} -> {}", key, statInfo.getPath());
            
        } catch (Exception e) {
            log.error("保存元数据失败: {}", path, e);
            throw new RuntimeException("保存元数据失败", e);
        }
    }
    
    /**
     * 获取元数据
     */
    public StatInfo getMetadata(String path) {
        try {
            String key = path;
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            
            if (value == null) {
                return null;
            }
            
            String json = new String(value, StandardCharsets.UTF_8);
            StatInfo statInfo = objectMapper.readValue(json, StatInfo.class);
            
            log.debug("获取元数据成功: {} -> {}", key, statInfo.getPath());
            return statInfo;
            
        } catch (Exception e) {
            log.error("获取元数据失败: {}", path, e);
            return null;
        }
    }
    
    /**
     * 检查路径是否存在
     */
    public boolean exists(String path) {
        try {
            String key = path;
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            return value != null;
        } catch (Exception e) {
            log.error("检查路径存在性失败: {}", path, e);
            return false;
        }
    }
    
    /**
     * 列出目录内容
     */
    public List<StatInfo> listDirectory(String parentPath) {
        List<StatInfo> children = new ArrayList<>();
        
        try {
            String parentPathWithSlash = parentPath.endsWith("/") ? parentPath : parentPath + "/";
            
            for (String path : allPaths) {
                if (path.startsWith(parentPathWithSlash) && !path.equals(parentPathWithSlash)) {
                    // 检查是否是直接子项（不是孙子项）
                    String relativePath = path.substring(parentPathWithSlash.length());
                    if (!relativePath.contains("/")) {
                        // 直接子项
                        StatInfo statInfo = getMetadata(path);
                        if (statInfo != null) {
                            children.add(statInfo);
                        }
                    }
                }
            }
            
            log.debug("列出目录内容: {} -> {} 个项目", parentPath, children.size());
            
        } catch (Exception e) {
            log.error("列出目录内容失败: {}", parentPath, e);
        }
        
        return children;
    }
    
    /**
     * 删除元数据
     */
    public void deleteMetadata(String path) {
        try {
            String key = path;
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            
            // 更新缓存
            allPaths.remove(path);
            
            log.debug("删除元数据成功: {}", path);
            
        } catch (Exception e) {
            log.error("删除元数据失败: {}", path, e);
            throw new RuntimeException("删除元数据失败", e);
        }
    }
    
    /**
     * 获取所有元数据
     */
    public List<StatInfo> getAllMetadata() {
        List<StatInfo> allMetadata = new ArrayList<>();
        
        try {
            for (String path : allPaths) {
                StatInfo statInfo = getMetadata(path);
                if (statInfo != null) {
                    allMetadata.add(statInfo);
                }
            }
            
            log.debug("获取所有元数据: {} 个项目", allMetadata.size());
            
        } catch (Exception e) {
            log.error("获取所有元数据失败", e);
        }
        
        return allMetadata;
    }
    
    // 兼容旧接口：删除全部元数据
    public void deleteAll() {
        try {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (String path : allPaths) {
                    writeBatch.delete(path.getBytes(StandardCharsets.UTF_8));
                }
                rocksDB.write(new WriteOptions(), writeBatch);
            }
            allPaths.clear();
            log.info("已删除所有元数据");
        } catch (Exception e) {
            log.error("删除所有元数据失败", e);
            throw new RuntimeException("删除所有元数据失败", e);
        }
    }
    
    /**
     * 获取存储统计信息
     */
    public Map<String, String> getStats() {
        Map<String, String> stats = new HashMap<>();
        
        try {
            stats.put("dbPath", dbPath);
            long totalFiles = allPaths.size();
            stats.put("totalFiles", String.valueOf(totalFiles));
            
            // 获取RocksDB统计信息
            String dbStats = rocksDB.getProperty("rocksdb.stats");
            if (dbStats != null) {
                stats.put("rocksdbStats", dbStats);
            }
            
        } catch (Exception e) {
            log.error("获取存储统计信息失败", e);
            stats.put("error", e.getMessage());
        }
        
        return stats;
    }
    
    @PreDestroy
    public void cleanup() {
        if (rocksDB != null) {
            rocksDB.close();
            log.info("RocksDB已关闭");
        }
    }
}
