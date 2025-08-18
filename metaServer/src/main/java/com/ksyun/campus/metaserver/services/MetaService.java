package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.FileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class MetaService {
    
    @Autowired
    private ZkDataServerService zkDataServerService;
    
    @Autowired
    private MetadataStorageService metadataStorage;
    
    @Autowired
    private DataServerClientService dataServerClient;
    

    
    // 内存中存储文件元数据（按文件系统名称隔离）
    private final Map<String, Map<String, StatInfo>> fileMetadataByFileSystem = new ConcurrentHashMap<>();
    
    // 每个文件系统的副本计数器
    private final Map<String, AtomicInteger> replicaCounters = new ConcurrentHashMap<>();
    
    /**
     * 获取文件系统的元数据缓存
     */
    private Map<String, StatInfo> getFileMetadata(String fileSystemName) {
        return fileMetadataByFileSystem.computeIfAbsent(fileSystemName, k -> new ConcurrentHashMap<>());
    }
    
    /**
     * 获取文件系统的副本计数器
     */
    private AtomicInteger getReplicaCounter(String fileSystemName) {
        return replicaCounters.computeIfAbsent(fileSystemName, k -> new AtomicInteger(0));
    }
    
    /**
     * 选择数据服务器（负载均衡）
     */
    public Object pickDataServer(String fileSystemName) {
        // 从ZK中获取活跃的数据服务器
        List<Map<String, Object>> activeServers = zkDataServerService.getActiveDataServers();
        
        if (activeServers.isEmpty()) {
            log.warn("文件系统 {} 没有可用的数据服务器", fileSystemName);
            return null;
        }
        
        // 使用轮询策略选择数据服务器
        AtomicInteger counter = getReplicaCounter(fileSystemName);
        int index = counter.getAndIncrement() % activeServers.size();
        return activeServers.get(index);
    }
    

    
    /**
     * 创建文件或目录
     */
    public StatInfo createFile(String fileSystemName, String path, FileType type) {
        // 检查路径是否已存在
        if (metadataStorage.exists(fileSystemName, path)) {
            log.warn("文件系统 {} 中文件/目录已存在: {}", fileSystemName, path);
            return getFile(fileSystemName, path);
        }
        
        // 确保父目录存在
        String parentPath = getParentPath(path);
        if (!parentPath.equals("/") && !metadataStorage.exists(fileSystemName, parentPath)) {
            log.warn("文件系统 {} 中父目录不存在，先创建父目录: {}", fileSystemName, parentPath);
            createFile(fileSystemName, parentPath, FileType.Directory);
        }
        
        StatInfo statInfo = new StatInfo();
        statInfo.setPath(path);
        statInfo.setSize(0);
        statInfo.setMtime(System.currentTimeMillis());
        statInfo.setType(type);
        
        // 保存到RocksDB和内存缓存
        metadataStorage.saveMetadata(fileSystemName, path, statInfo);
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        fileMetadata.put(path, statInfo);
        
        log.info("文件系统 {} 创建文件/目录: {}, 类型: {}", fileSystemName, path, type);
        return statInfo;
    }
    
    /**
     * 写入文件数据到DataServer并记录副本位置
     */
    public StatInfo writeFile(String fileSystemName, String path, byte[] data, int offset, int length) {
        try {
            // 1. 检查文件是否存在，不存在则创建
            StatInfo statInfo = getFile(fileSystemName, path);
            if (statInfo == null) {
                log.info("文件系统 {} 中文件不存在，先创建文件: {}", fileSystemName, path);
                statInfo = createFile(fileSystemName, path, FileType.File);
            }
            
            // 2. 选择DataServer进行写入
            Object selectedDataServer = pickDataServer(fileSystemName);
            if (selectedDataServer == null) {
                throw new RuntimeException("没有可用的DataServer");
            }
            
            // 3. 调用DataServer的write接口
            Map<String, Object> writeResult = dataServerClient.writeToDataServer(
                selectedDataServer, fileSystemName, path, offset, length, data);
            
            if (!(Boolean) writeResult.get("success")) {
                throw new RuntimeException("DataServer写入失败: " + writeResult.get("message"));
            }
            
            // 4. 获取并记录副本位置信息
            @SuppressWarnings("unchecked")
            List<String> replicaLocations = (List<String>) writeResult.get("replicaLocations");
            if (replicaLocations != null && !replicaLocations.isEmpty()) {
                // 将副本位置转换为ReplicaData格式
                List<ReplicaData> replicaDataList = convertToReplicaData(path, replicaLocations, offset, length);
                statInfo.setReplicaData(replicaDataList);
                
                // 更新文件大小
                statInfo.setSize(offset + length);
                statInfo.setMtime(System.currentTimeMillis());
                
                // 保存更新后的元数据
                metadataStorage.saveMetadata(fileSystemName, path, statInfo);
                Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
                fileMetadata.put(path, statInfo);
                
                log.info("文件系统 {} 写入文件成功: {}, 大小: {}, 副本位置: {}", 
                        fileSystemName, path, statInfo.getSize(), replicaLocations);
            }
            
            return statInfo;
            
        } catch (Exception e) {
            log.error("文件系统 {} 写入文件失败: {}", fileSystemName, path, e);
            throw new RuntimeException("写入文件失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 将副本位置列表转换为ReplicaData格式
     */
    private List<ReplicaData> convertToReplicaData(String filePath, List<String> replicaLocations, int offset, int length) {
        List<ReplicaData> replicaDataList = new ArrayList<>();
        
        for (int i = 0; i < replicaLocations.size(); i++) {
            String location = replicaLocations.get(i);
            String[] parts = location.split(":");
            if (parts.length == 2) {
                ReplicaData replica = new ReplicaData();
                replica.id = UUID.randomUUID().toString();
                replica.dsNode = location; // 格式：ip:port
                replica.path = filePath;
                replica.offset = offset;
                replica.length = length;
                replica.isPrimary = i == 0; // 第一个副本为主副本
                replicaDataList.add(replica);
            }
        }
        
        return replicaDataList;
    }
    

    
    /**
     * 获取文件信息
     */
    public StatInfo getFile(String fileSystemName, String path) {
        // 先从内存缓存获取，如果没有则从RocksDB获取
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        StatInfo statInfo = fileMetadata.get(path);
        if (statInfo == null) {
            statInfo = metadataStorage.getMetadata(fileSystemName, path);
            if (statInfo != null) {
                fileMetadata.put(path, statInfo); // 更新缓存
            }
        }
        return statInfo;
    }
    
    /**
     * 列出目录内容
     */
    public List<StatInfo> listFiles(String fileSystemName, String parentPath) {
        // 使用RocksDB的目录列表功能
        return metadataStorage.listDirectory(fileSystemName, parentPath);
    }
    
    /**
     * 获取父目录路径
     */
    private String getParentPath(String path) {
        if (path.equals("/")) {
            return "/";
        }
        int lastSlashIndex = path.lastIndexOf('/');
        if (lastSlashIndex == 0) {
            return "/";
        }
        return path.substring(0, lastSlashIndex);
    }
    
    /**
     * 删除文件或目录（支持递归删除非空目录）
     */
    public boolean deleteFile(String fileSystemName, String path) {
        StatInfo statInfo = getFile(fileSystemName, path);
        if (statInfo == null) {
            log.warn("文件系统 {} 中文件/目录不存在: {}", fileSystemName, path);
            return false;
        }
        
        try {
            if (statInfo.getType() == FileType.Directory) {
                // 目录删除，先递归删除子文件
                log.info("文件系统 {} 开始递归删除目录: {}", fileSystemName, path);
                boolean recursiveDeleteSuccess = deleteDirectoryRecursively(fileSystemName, path);
                if (!recursiveDeleteSuccess) {
                    log.error("文件系统 {} 递归删除目录失败: {}", fileSystemName, path);
                    return false;
                }
            } else {
                // 文件删除：DataServer会自动管理副本删除，这里只删除元数据
                log.info("文件系统 {} 删除文件: {}, 元数据删除", fileSystemName, path);
            }
            
            // 删除成功后，从RocksDB和内存缓存中删除元数据
            metadataStorage.deleteMetadata(fileSystemName, path);
            Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
            fileMetadata.remove(path);
            log.info("文件系统 {} 成功删除文件/目录: {}", fileSystemName, path);
            return true;
            
        } catch (Exception e) {
            log.error("文件系统 {} 删除文件/目录异常: {}", fileSystemName, path, e);
            return false;
        }
    }
    
    /**
     * 递归删除目录及其所有内容
     */
    private boolean deleteDirectoryRecursively(String fileSystemName, String dirPath) {
        try {
            List<StatInfo> children = listFiles(fileSystemName, dirPath);
            if (children.isEmpty()) {
                log.debug("文件系统 {} 中目录为空，直接删除: {}", fileSystemName, dirPath);
                return true;
            }
            
            log.info("文件系统 {} 中目录 {} 包含 {} 个子项，开始递归删除", 
                    fileSystemName, dirPath, children.size());
            
            // 先删除所有子项
            for (StatInfo child : children) {
                String childPath = child.getPath();
                if (child.getType() == FileType.Directory) {
                    // 递归删除子目录
                    if (!deleteDirectoryRecursively(fileSystemName, childPath)) {
                        log.error("文件系统 {} 递归删除子目录失败: {}", fileSystemName, childPath);
                        return false;
                    }
                } else {
                    // 删除子文件，先调用DataServer删除实际数据
                    List<ReplicaData> replicas = child.getReplicaData();
                    if (replicas != null && !replicas.isEmpty()) {
                        int deletedCount = dataServerClient.deleteFromMultipleDataServers(replicas);
                        log.info("文件系统 {} 删除子文件: {}, 在DataServer上成功删除: {}/{}", 
                                fileSystemName, childPath, deletedCount, replicas.size());
                    }
                }
                
                // 删除子项的元数据
                metadataStorage.deleteMetadata(fileSystemName, childPath);
                Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
                fileMetadata.remove(childPath);
            }
            
            log.info("文件系统 {} 中目录 {} 的所有子项删除完成", fileSystemName, dirPath);
            return true;
            
        } catch (Exception e) {
            log.error("文件系统 {} 递归删除目录异常: {}", fileSystemName, dirPath, e);
            return false;
        }
    }
    

    
    /**
     * 获取所有数据服务器
     */
    public List<Object> getDataServers() {
        return new ArrayList<>(zkDataServerService.getAllDataServers().values());
    }
    

    
    /**
     * 获取所有文件元数据
     */
    public List<StatInfo> getAllFiles(String fileSystemName) {
        return metadataStorage.getAllMetadata(fileSystemName);
    }
    
    /**
     * 更新数据服务器心跳
     */
    public void updateDataServerHeartbeat(String serverId) {
        zkDataServerService.updateDataServerHeartbeat(serverId);
    }
    
    /**
     * 标记数据服务器为不可用
     */
    public void markDataServerInactive(String serverId) {
        zkDataServerService.markDataServerInactive(serverId);
    }
    
    /**
     * 检查文件是否存在
     */
    public boolean fileExists(String fileSystemName, String path) {
        return metadataStorage.exists(fileSystemName, path);
    }
    
    /**
     * 获取文件状态信息
     */
    public StatInfo getFileStatus(String fileSystemName, String path) {
        return getFile(fileSystemName, path);
    }
    
    /**
     * 创建目录
     */
    public StatInfo createDirectory(String fileSystemName, String path) {
        return createFile(fileSystemName, path, FileType.Directory);
    }
    
    /**
     * 重命名文件或目录
     */
    public boolean renameFile(String fileSystemName, String oldPath, String newPath) {
        StatInfo statInfo = getFile(fileSystemName, oldPath);
        if (statInfo == null) {
            log.warn("文件系统 {} 中源文件/目录不存在: {}", fileSystemName, oldPath);
            return false;
        }
        
        if (metadataStorage.exists(fileSystemName, newPath)) {
            log.warn("文件系统 {} 中目标路径已存在: {}", fileSystemName, newPath);
            return false;
        }
        
        // 创建新的元数据
        StatInfo newStatInfo = new StatInfo();
        newStatInfo.setPath(newPath);
        newStatInfo.setSize(statInfo.getSize());
        newStatInfo.setMtime(System.currentTimeMillis());
        newStatInfo.setType(statInfo.getType());
        newStatInfo.setReplicaData(statInfo.getReplicaData());
        
        // 保存新路径的元数据
        metadataStorage.saveMetadata(fileSystemName, newPath, newStatInfo);
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        fileMetadata.put(newPath, newStatInfo);
        
        // 删除旧路径的元数据
        deleteFile(fileSystemName, oldPath);
        
        log.info("文件系统 {} 重命名文件/目录: {} -> {}", fileSystemName, oldPath, newPath);
        return true;
    }
    
    /**
     * 获取DataServer状态信息
     */
    public Map<String, Object> getDataServerStatus() {
        return dataServerClient.getDataServerStatus();
    }
    
    /**
     * 获取文件系统统计信息
     */
    public Map<String, Object> getFileSystemStats(String fileSystemName) {
        Map<String, Object> stats = new HashMap<>();
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        
        stats.put("fileSystemName", fileSystemName);
        stats.put("totalFiles", fileMetadata.size());
        stats.put("totalDirectories", (int) fileMetadata.values().stream()
                .filter(f -> f.getType() == FileType.Directory).count());
        stats.put("totalRegularFiles", (int) fileMetadata.values().stream()
                .filter(f -> f.getType() == FileType.File).count());
        
        // 计算总存储大小
        long totalSize = fileMetadata.values().stream()
                .filter(f -> f.getType() == FileType.File)
                .mapToLong(StatInfo::getSize)
                .sum();
        stats.put("totalSize", totalSize);
        
        return stats;
    }
}
