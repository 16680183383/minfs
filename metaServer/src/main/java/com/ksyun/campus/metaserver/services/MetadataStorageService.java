package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
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
    private final String dbPath = "./rocksdb_metadata";
    
    // 缓存文件系统信息
    private final Map<String, Set<String>> fileSystemPaths = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void init() {
        try {
            File dbDir = new File(dbPath);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            
            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setMaxBackgroundJobs(4);
            options.setMaxWriteBufferNumber(4);
            options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
            options.setLevel0SlowdownWritesTrigger(8);
            options.setLevel0StopWritesTrigger(12);
            options.setMaxWriteBufferNumber(4);
            options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
            
            rocksDB = RocksDB.open(options, dbPath);
            log.info("RocksDB初始化成功，路径: {}", dbPath);
            
            // 重建文件系统路径缓存
            rebuildFileSystemCache();
            
        } catch (RocksDBException e) {
            log.error("RocksDB初始化失败", e);
            throw new RuntimeException("RocksDB初始化失败", e);
        }
    }
    
    /**
     * 重建文件系统路径缓存
     */
    private void rebuildFileSystemCache() {
        try {
            fileSystemPaths.clear();
            RocksIterator iterator = rocksDB.newIterator();
            iterator.seekToFirst();
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                // 键格式: fileSystemName:path
                String[] parts = key.split(":", 2);
                if (parts.length == 2) {
                    String fileSystemName = parts[0];
                    String path = parts[1];
                    fileSystemPaths.computeIfAbsent(fileSystemName, k -> new HashSet<>()).add(path);
                }
                iterator.next();
            }
            iterator.close();
            
            log.info("文件系统路径缓存重建完成，共 {} 个文件系统", fileSystemPaths.size());
        } catch (Exception e) {
            log.error("重建文件系统路径缓存失败", e);
        }
    }
    
    /**
     * 保存元数据
     */
    public void saveMetadata(String fileSystemName, String path, StatInfo statInfo) {
        try {
            String key = fileSystemName + ":" + path;
            String json = objectMapper.writeValueAsString(statInfo);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            
            rocksDB.put(key.getBytes(StandardCharsets.UTF_8), value);
            
            // 更新缓存
            fileSystemPaths.computeIfAbsent(fileSystemName, k -> new HashSet<>()).add(path);
            
            log.debug("保存元数据成功: {} -> {}", key, statInfo.getPath());
            
        } catch (Exception e) {
            log.error("保存元数据失败: {}:{}", fileSystemName, path, e);
            throw new RuntimeException("保存元数据失败", e);
        }
    }
    
    /**
     * 获取元数据
     */
    public StatInfo getMetadata(String fileSystemName, String path) {
        try {
            String key = fileSystemName + ":" + path;
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            
            if (value == null) {
                return null;
            }
            
            String json = new String(value, StandardCharsets.UTF_8);
            StatInfo statInfo = objectMapper.readValue(json, StatInfo.class);
            
            log.debug("获取元数据成功: {} -> {}", key, statInfo.getPath());
            return statInfo;
            
        } catch (Exception e) {
            log.error("获取元数据失败: {}:{}", fileSystemName, path, e);
            return null;
        }
    }
    
    /**
     * 检查路径是否存在
     */
    public boolean exists(String fileSystemName, String path) {
        try {
            String key = fileSystemName + ":" + path;
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            return value != null;
        } catch (Exception e) {
            log.error("检查路径存在性失败: {}:{}", fileSystemName, path, e);
            return false;
        }
    }
    
    /**
     * 列出目录内容
     */
    public List<StatInfo> listDirectory(String fileSystemName, String parentPath) {
        List<StatInfo> children = new ArrayList<>();
        
        try {
            Set<String> paths = fileSystemPaths.get(fileSystemName);
            if (paths == null) {
                return children;
            }
            
            String parentPathWithSlash = parentPath.endsWith("/") ? parentPath : parentPath + "/";
            
            for (String path : paths) {
                if (path.startsWith(parentPathWithSlash) && !path.equals(parentPathWithSlash)) {
                    // 检查是否是直接子项（不是孙子项）
                    String relativePath = path.substring(parentPathWithSlash.length());
                    if (!relativePath.contains("/")) {
                        // 直接子项
                        StatInfo statInfo = getMetadata(fileSystemName, path);
                        if (statInfo != null) {
                            children.add(statInfo);
                        }
                    }
                }
            }
            
            log.debug("列出目录内容: {}:{} -> {} 个项目", fileSystemName, parentPath, children.size());
            
        } catch (Exception e) {
            log.error("列出目录内容失败: {}:{}", fileSystemName, parentPath, e);
        }
        
        return children;
    }
    
    /**
     * 删除元数据
     */
    public void deleteMetadata(String fileSystemName, String path) {
        try {
            String key = fileSystemName + ":" + path;
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            
            // 更新缓存
            Set<String> paths = fileSystemPaths.get(fileSystemName);
            if (paths != null) {
                paths.remove(path);
            }
            
            log.debug("删除元数据成功: {}:{}", fileSystemName, path);
            
        } catch (Exception e) {
            log.error("删除元数据失败: {}:{}", fileSystemName, path, e);
            throw new RuntimeException("删除元数据失败", e);
        }
    }
    
    /**
     * 获取所有元数据
     */
    public List<StatInfo> getAllMetadata(String fileSystemName) {
        List<StatInfo> allMetadata = new ArrayList<>();
        
        try {
            Set<String> paths = fileSystemPaths.get(fileSystemName);
            if (paths == null) {
                return allMetadata;
            }
            
            for (String path : paths) {
                StatInfo statInfo = getMetadata(fileSystemName, path);
                if (statInfo != null) {
                    allMetadata.add(statInfo);
                }
            }
            
            log.debug("获取所有元数据: {} -> {} 个项目", fileSystemName, allMetadata.size());
            
        } catch (Exception e) {
            log.error("获取所有元数据失败: {}", fileSystemName, e);
        }
        
        return allMetadata;
    }
    
    /**
     * 批量删除文件系统下的所有元数据
     */
    public void deleteFileSystem(String fileSystemName) {
        try {
            Set<String> paths = fileSystemPaths.get(fileSystemName);
            if (paths == null) {
                return;
            }
            
            log.info("开始删除文件系统: {}, 包含 {} 个项目", fileSystemName, paths.size());
            
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (String path : paths) {
                    String key = fileSystemName + ":" + path;
                    writeBatch.delete(key.getBytes(StandardCharsets.UTF_8));
                }
                
                rocksDB.write(new WriteOptions(), writeBatch);
            }
            
            // 清理缓存
            fileSystemPaths.remove(fileSystemName);
            
            log.info("文件系统删除完成: {}", fileSystemName);
            
        } catch (Exception e) {
            log.error("删除文件系统失败: {}", fileSystemName, e);
            throw new RuntimeException("删除文件系统失败", e);
        }
    }
    
    /**
     * 获取存储统计信息
     */
    public Map<String, String> getStats() {
        Map<String, String> stats = new HashMap<>();
        
        try {
            stats.put("dbPath", dbPath);
            stats.put("totalFileSystems", String.valueOf(fileSystemPaths.size()));
            
            long totalFiles = fileSystemPaths.values().stream()
                    .mapToLong(Set::size)
                    .sum();
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
    
    /**
     * 获取文件系统列表
     */
    public List<String> getFileSystemList() {
        return new ArrayList<>(fileSystemPaths.keySet());
    }
    
    /**
     * 获取文件系统统计信息
     */
    public Map<String, Object> getFileSystemStats(String fileSystemName) {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            Set<String> paths = fileSystemPaths.get(fileSystemName);
            if (paths == null) {
                stats.put("fileSystemName", fileSystemName);
                stats.put("totalFiles", 0);
                stats.put("totalDirectories", 0);
                stats.put("totalRegularFiles", 0);
                stats.put("totalSize", 0);
                return stats;
            }
            
            int totalFiles = 0;
            int totalDirectories = 0;
            int totalRegularFiles = 0;
            long totalSize = 0;
            
            for (String path : paths) {
                StatInfo statInfo = getMetadata(fileSystemName, path);
                if (statInfo != null) {
                    totalFiles++;
                    if (statInfo.getType().getCode() == 1) { // Directory
                        totalDirectories++;
                    } else if (statInfo.getType().getCode() == 2) { // File
                        totalRegularFiles++;
                        totalSize += statInfo.getSize();
                    }
                }
            }
            
            stats.put("fileSystemName", fileSystemName);
            stats.put("totalFiles", totalFiles);
            stats.put("totalDirectories", totalDirectories);
            stats.put("totalRegularFiles", totalRegularFiles);
            stats.put("totalSize", totalSize);
            
        } catch (Exception e) {
            log.error("获取文件系统统计信息失败: {}", fileSystemName, e);
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
