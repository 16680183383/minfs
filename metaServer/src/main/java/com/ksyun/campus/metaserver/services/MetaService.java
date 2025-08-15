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
     * 为文件创建三副本
     */
    public List<ReplicaData> createReplicas(String fileSystemName, String filePath, long fileSize) {
        List<ReplicaData> replicas = new ArrayList<>();
        List<Map<String, Object>> availableServers = zkDataServerService.getActiveDataServers();
        
        if (availableServers.size() < 3) {
            log.error("文件系统 {} 可用数据服务器数量不足，需要至少3个，当前只有{}个", 
                     fileSystemName, availableServers.size());
            return replicas;
        }
        
        // 选择3个不同的数据服务器，确保均匀分布
        Set<Map<String, Object>> selectedServers = new HashSet<>();
        
        // 优先选择副本数量较少的服务器
        Map<String, Integer> replicaCounts = getReplicaDistribution(fileSystemName);
        List<Map<String, Object>> sortedServers = new ArrayList<>(availableServers);
        sortedServers.sort((a, b) -> {
            String idA = (String) a.get("id");
            String idB = (String) b.get("id");
            int countA = replicaCounts.getOrDefault(idA, 0);
            int countB = replicaCounts.getOrDefault(idB, 0);
            return Integer.compare(countA, countB);
        });
        
        // 选择前3个副本数量最少的服务器
        for (int i = 0; i < Math.min(3, sortedServers.size()); i++) {
            selectedServers.add(sortedServers.get(i));
        }
        
        // 创建3个副本
        int replicaIndex = 0;
        for (Map<String, Object> server : selectedServers) {
            ReplicaData replica = new ReplicaData();
            replica.id = UUID.randomUUID().toString();
            replica.dsNode = (String) server.get("address");
            // 在副本路径中包含文件系统名称，实现隔离
            replica.path = "/data/" + server.get("id") + "/" + fileSystemName + "/" + filePath.replace("/", "_");
            replica.offset = 0;
            replica.length = fileSize;
            replica.isPrimary = replicaIndex == 0; // 第一个副本为主副本
            replicas.add(replica);
            replicaIndex++;
        }
        
        log.info("文件系统 {} 为文件 {} 创建了 {} 个副本", fileSystemName, filePath, replicas.size());
        return replicas;
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
        
        if (type == FileType.File) {
            // 为文件创建副本
            statInfo.setReplicaData(createReplicas(fileSystemName, path, 0));
        }
        
        // 保存到RocksDB和内存缓存
        metadataStorage.saveMetadata(fileSystemName, path, statInfo);
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        fileMetadata.put(path, statInfo);
        log.info("文件系统 {} 创建文件/目录: {}, 类型: {}", fileSystemName, path, type);
        return statInfo;
    }
    
    /**
     * 写入文件数据到DataServer（三副本同步）
     */
    public int writeFileData(String fileSystemName, String path, byte[] data, int offset, int length) {
        StatInfo statInfo = getFile(fileSystemName, path);
        if (statInfo == null || statInfo.getType() != FileType.File) {
            log.error("文件系统 {} 中文件不存在或不是文件类型: {}", fileSystemName, path);
            return 0;
        }
        
        List<ReplicaData> replicas = statInfo.getReplicaData();
        if (replicas == null || replicas.isEmpty()) {
            log.error("文件系统 {} 中文件没有副本信息: {}", fileSystemName, path);
            return 0;
        }
        
        // 调用DataServerClientService写入三副本
        int successCount = dataServerClient.writeToMultipleDataServers(replicas, data, offset, length);
        
        if (successCount >= 2) { // 至少2个副本成功才认为写入成功
            // 更新文件大小
            updateFileSize(fileSystemName, path, offset + length);
            log.info("文件系统 {} 中文件写入成功: {}, 副本数: {}/{}", 
                    fileSystemName, path, successCount, replicas.size());
        } else {
            log.error("文件系统 {} 中文件写入失败: {}, 成功副本数: {}/{}", 
                    fileSystemName, path, successCount, replicas.size());
        }
        
        return successCount;
    }
    
    /**
     * 从DataServer读取文件数据
     */
    public byte[] readFileData(String fileSystemName, String path, int offset, int length) {
        StatInfo statInfo = getFile(fileSystemName, path);
        if (statInfo == null || statInfo.getType() != FileType.File) {
            log.error("文件系统 {} 中文件不存在或不是文件类型: {}", fileSystemName, path);
            return null;
        }
        
        List<ReplicaData> replicas = statInfo.getReplicaData();
        if (replicas == null || replicas.isEmpty()) {
            log.error("文件系统 {} 中文件没有副本信息: {}", fileSystemName, path);
            return null;
        }
        
        // 调用DataServerClientService读取数据（支持故障转移）
        return dataServerClient.readFromMultipleDataServers(replicas, offset, length);
    }
    
    /**
     * 删除文件数据（从所有DataServer删除）
     */
    public int deleteFileData(String fileSystemName, String path) {
        StatInfo statInfo = getFile(fileSystemName, path);
        if (statInfo == null) {
            log.warn("文件系统 {} 中文件不存在: {}", fileSystemName, path);
            return 0;
        }
        
        if (statInfo.getType() == FileType.Directory) {
            // 目录删除，递归删除子文件
            List<StatInfo> children = listFiles(fileSystemName, path);
            int totalDeleted = 0;
            for (StatInfo child : children) {
                totalDeleted += deleteFileData(fileSystemName, child.getPath());
            }
            return totalDeleted;
        }
        
        // 文件删除，从所有DataServer删除副本
        List<ReplicaData> replicas = statInfo.getReplicaData();
        if (replicas == null || replicas.isEmpty()) {
            log.warn("文件系统 {} 中文件没有副本信息: {}", fileSystemName, path);
            return 0;
        }
        
        int successCount = 0;
        for (ReplicaData replica : replicas) {
            if (dataServerClient.deleteFromDataServer(replica)) {
                successCount++;
            }
        }
        
        log.info("文件系统 {} 中文件删除完成: {}, 成功删除副本数: {}/{}", 
                fileSystemName, path, successCount, replicas.size());
        return successCount;
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
                // 文件删除，从所有DataServer删除副本
                int deletedCount = deleteFileData(fileSystemName, path);
                if (deletedCount == 0) {
                    log.error("文件系统 {} 文件删除失败: {}", fileSystemName, path);
                    return false;
                }
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
                    // 删除子文件
                    int deletedCount = deleteFileData(fileSystemName, childPath);
                    if (deletedCount == 0) {
                        log.error("文件系统 {} 删除子文件失败: {}", fileSystemName, childPath);
                        return false;
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
     * 更新文件大小
     */
    public void updateFileSize(String fileSystemName, String path, long size) {
        StatInfo statInfo = getFile(fileSystemName, path);
        if (statInfo != null) {
            statInfo.setSize(size);
            statInfo.setMtime(System.currentTimeMillis());
            // 更新到RocksDB和内存缓存
            metadataStorage.saveMetadata(fileSystemName, path, statInfo);
            Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
            fileMetadata.put(path, statInfo);
            log.info("文件系统 {} 更新文件大小: {}, 新大小: {}", fileSystemName, path, size);
        }
    }
    
    /**
     * 获取所有数据服务器
     */
    public List<Object> getDataServers() {
        return new ArrayList<>(zkDataServerService.getAllDataServers().values());
    }
    
    /**
     * 获取副本分布情况
     */
    public Map<String, Integer> getReplicaDistribution(String fileSystemName) {
        Map<String, Integer> distribution = new HashMap<>();
        Map<String, Map<String, Object>> allServers = zkDataServerService.getAllDataServers();
        
        // 初始化所有数据服务器的副本计数
        for (String serverId : allServers.keySet()) {
            distribution.put(serverId, 0);
        }
        
        // 统计每个数据服务器上的副本数量
        Map<String, StatInfo> fileMetadata = getFileMetadata(fileSystemName);
        for (StatInfo statInfo : fileMetadata.values()) {
            if (statInfo.getReplicaData() != null) {
                for (ReplicaData replica : statInfo.getReplicaData()) {
                    String serverId = extractServerId(replica.dsNode);
                    distribution.put(serverId, distribution.getOrDefault(serverId, 0) + 1);
                }
            }
        }
        
        return distribution;
    }
    
    /**
     * 从数据服务器地址提取服务器ID
     */
    private String extractServerId(String dsNode) {
        // 从 "localhost:9001" 提取 "ds1"
        if (dsNode.contains(":9001")) return "ds1";
        if (dsNode.contains(":9002")) return "ds2";
        if (dsNode.contains(":9003")) return "ds3";
        if (dsNode.contains(":9004")) return "ds4";
        return "unknown";
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
