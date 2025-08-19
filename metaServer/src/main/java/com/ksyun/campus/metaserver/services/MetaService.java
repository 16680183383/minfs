package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.FileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
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
    
    @Autowired
    private ReplicationService replicationService;
    
    // 简单轮询计数器（全局）
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    /**
     * 选择数据服务器（负载均衡）
     */
    public Object pickDataServer() {
        // 从ZK中获取活跃的数据服务器
        List<Map<String, Object>> activeServers = zkDataServerService.getActiveDataServers();
        
        if (activeServers.isEmpty()) {
            log.warn("没有可用的数据服务器");
            return null;
        }
        
        // 使用轮询策略选择数据服务器
        int index = Math.floorMod(roundRobinCounter.getAndIncrement(), activeServers.size());
        return activeServers.get(index);
    }
    

    
    /**
     * 创建文件或目录
     */
    public StatInfo createFile(String path, FileType type) {
        // 检查路径是否已存在
        if (metadataStorage.exists(path)) {
            log.warn("文件/目录已存在: {}", path);
            return getFile(path);
        }
        
        // 确保父目录存在
        String parentPath = getParentPath(path);
        if (!parentPath.equals("/") && !metadataStorage.exists(parentPath)) {
            log.warn("父目录不存在，先创建父目录: {}", parentPath);
            createFile(parentPath, FileType.Directory);
        }
        
        StatInfo statInfo = new StatInfo();
        statInfo.setPath(path);
        statInfo.setSize(0);
        statInfo.setMtime(System.currentTimeMillis());
        statInfo.setType(type);
        
        // 如果是普通文件，选择三台DataServer并设置副本信息（不实际创建文件）
        if (type == FileType.File) {
            try {
                // 选择三台DataServer（轮询 + 剩余容量权重）
                List<Map<String, Object>> allActive = new ArrayList<>(zkDataServerService.getActiveDataServers());
                if (allActive.size() < 1) {
                    throw new RuntimeException("没有可用的DataServer");
                }
                
                // 按剩余容量排序
                allActive.sort((a, b) -> Long.compare(
                        ((Number) b.getOrDefault("totalCapacity", b.getOrDefault("capacity", 0L))).longValue() -
                                ((Number) b.getOrDefault("usedCapacity", 0L)).longValue(),
                        ((Number) a.getOrDefault("totalCapacity", a.getOrDefault("capacity", 0L))).longValue() -
                                ((Number) a.getOrDefault("usedCapacity", 0L)).longValue()
                ));
                
                // 轮询选择三台DataServer
                int start = Math.floorMod(roundRobinCounter.getAndIncrement(), Math.max(1, allActive.size()));
                List<String> targets = new ArrayList<>();
                for (int i = 0; i < Math.min(3, allActive.size()); i++) {
                    Map<String, Object> ds = allActive.get((start + i) % allActive.size());
                    Object addr = ds.get("address");
                    if (addr != null) {
                        targets.add(String.valueOf(addr));
                    }
                }
                
                if (targets.isEmpty()) {
                    throw new RuntimeException("无法选择到目标DataServer");
                }
                
                // 设置副本信息（不实际创建文件）
                List<ReplicaData> replicaDataList = convertToReplicaData(path, targets, 0, 0);
                statInfo.setReplicaData(replicaDataList);
                log.info("创建文件成功: {}, 选择的三副本位置: {}", path, targets);
                
            } catch (Exception e) {
                log.error("创建文件时选择DataServer失败: {}", path, e);
                // 即使选择失败，也要保存元数据，后续write时可以重试
            }
        }

        // 保存到RocksDB和内存缓存
        metadataStorage.saveMetadata(path, statInfo);
        
        log.info("创建文件/目录: {}, 类型: {}", path, type);
        return statInfo;
    }
    
    /**
     * 读取文件数据
     * 支持分块读取，从DataServer获取文件内容
     */
    public byte[] readFile(String path, int offset, int length) {
        try {
            // 1. 获取文件元数据
            StatInfo statInfo = getFile(path);
            if (statInfo == null) {
                log.warn("文件不存在: {}", path);
                return null;
            }
            
            if (statInfo.getType() == FileType.Directory) {
                log.warn("尝试读取目录: {}", path);
                return null;
            }
            
            // 2. 检查副本信息
            if (statInfo.getReplicaData() == null || statInfo.getReplicaData().isEmpty()) {
                log.warn("文件没有可用的副本: {}", path);
                return null;
            }
            
            // 3. 从第一个可用副本读取数据
            ReplicaData primaryReplica = statInfo.getReplicaData().get(0);
            String replicaAddress = primaryReplica.dsNode;
            
            log.info("从副本读取文件: {} -> {}", path, replicaAddress);
            
            // 4. 调用DataServer读取数据
            byte[] data = dataServerClient.readFromDataServer(replicaAddress, path);
            if (data == null) {
                log.warn("从DataServer读取失败，尝试其他副本: {}", replicaAddress);
                
                // 尝试其他副本
                for (int i = 1; i < statInfo.getReplicaData().size(); i++) {
                    ReplicaData replica = statInfo.getReplicaData().get(i);
                    data = dataServerClient.readFromDataServer(replica.dsNode, path);
                    if (data != null) {
                        log.info("从备用副本读取成功: {}", replica.dsNode);
                        break;
                    }
                }
            }
            
            if (data == null) {
                log.error("所有副本都无法读取文件: {}", path);
                return null;
            }
            
            // 5. 处理分块读取
            if (offset > 0 || length > 0) {
                int actualLength = length > 0 ? length : data.length - offset;
                if (offset >= data.length) {
                    log.warn("偏移量超出文件大小: offset={}, fileSize={}", offset, data.length);
                    return new byte[0];
                }
                
                int endPos = Math.min(offset + actualLength, data.length);
                int resultLength = endPos - offset;
                
                byte[] result = new byte[resultLength];
                System.arraycopy(data, offset, result, 0, resultLength);
                
                log.info("分块读取成功: path={}, offset={}, length={}, actualLength={}", 
                        path, offset, actualLength, resultLength);
                return result;
            }
            
            log.info("读取文件成功: path={}, size={}", path, data.length);
            return data;
            
        } catch (Exception e) {
            log.error("读取文件失败: path={}", path, e);
            return null;
        }
    }

    /**
     * 写入文件数据到DataServer并记录副本位置
     */
    public StatInfo writeFile(String path, byte[] data, int offset, int length) {
        try {
            // 1. 检查文件是否存在，不存在则创建
            StatInfo statInfo = getFile(path);
            if (statInfo == null) {
                log.info("文件不存在，先创建文件: {}", path);
                statInfo = createFile(path, FileType.File);
            }

            // 2. 优先使用已存在的副本位置，如果没有则重新选择
            List<String> targets = new ArrayList<>();
            if (statInfo.getReplicaData() != null && !statInfo.getReplicaData().isEmpty()) {
                // 使用已存在的副本位置
                for (ReplicaData replica : statInfo.getReplicaData()) {
                    targets.add(replica.dsNode);
                }
                log.info("使用已存在的副本位置: {}", targets);
            } else {
                // 重新选择三台DataServer（轮询 + 剩余容量权重）
                List<Map<String, Object>> allActive = new ArrayList<>(zkDataServerService.getActiveDataServers());
                if (allActive.size() < 1) {
                    throw new RuntimeException("没有可用的DataServer");
                }
                
                // 按剩余容量排序
                allActive.sort((a, b) -> Long.compare(
                        ((Number) b.getOrDefault("totalCapacity", b.getOrDefault("capacity", 0L))).longValue() -
                                ((Number) b.getOrDefault("usedCapacity", 0L)).longValue(),
                        ((Number) a.getOrDefault("totalCapacity", a.getOrDefault("capacity", 0L))).longValue() -
                                ((Number) a.getOrDefault("usedCapacity", 0L)).longValue()
                ));
                
                // 轮询选择三台DataServer
                int start = Math.floorMod(roundRobinCounter.getAndIncrement(), Math.max(1, allActive.size()));
                for (int i = 0; i < Math.min(3, allActive.size()); i++) {
                    Map<String, Object> ds = allActive.get((start + i) % allActive.size());
                    Object addr = ds.get("address");
                    if (addr != null) {
                        targets.add(String.valueOf(addr));
                    }
                }
                
                if (targets.isEmpty()) {
                    throw new RuntimeException("无法选择到目标DataServer");
                }
                
                log.info("重新选择DataServer: {}", targets);
            }

            // 3. 依次写入三台（第一台作为主副本）
            List<String> successLocations = new ArrayList<>();
            for (String addr : targets) {
                boolean ok = dataServerClient.writeDirectToDataServer(addr, path, offset, length, data);
                if (ok) {
                    successLocations.add(addr);
                }
            }
            if (successLocations.isEmpty()) {
                throw new RuntimeException("三副本写入均失败");
            }

            // 4. 更新副本信息与元数据
            List<ReplicaData> replicaDataList = convertToReplicaData(path, successLocations, offset, length);
            statInfo.setReplicaData(replicaDataList);
            
            // 修复：正确计算文件大小
            // 如果offset + length大于当前文件大小，则更新为新的文件大小
            // 如果offset + length小于等于当前文件大小，则保持当前大小不变
            long newSize = offset + length;
            if (newSize > statInfo.getSize()) {
                statInfo.setSize(newSize);
                log.info("更新文件大小: {} -> {}", path, newSize);
            } else {
                log.info("保持文件大小: {} -> {}", path, statInfo.getSize());
            }
            
            statInfo.setMtime(System.currentTimeMillis());
            metadataStorage.saveMetadata(path, statInfo);
            log.info("写入文件成功: {}, 大小: {}, 副本位置: {}", path, statInfo.getSize(), successLocations);

            return statInfo;
            
        } catch (Exception e) {
            log.error("写入文件失败: {}", path, e);
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
    public StatInfo getFile(String path) {
        // 从RocksDB获取
        return metadataStorage.getMetadata(path);
    }
    
    /**
     * 列出目录内容
     */
    public List<StatInfo> listFiles(String parentPath) {
        // 使用RocksDB的目录列表功能
        return metadataStorage.listDirectory(parentPath);
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
    public boolean deleteFile(String path) {
        StatInfo statInfo = getFile(path);
        // 若目录元数据不存在，但存在子项，则按目录处理进行递归删除
        if (statInfo == null) {
            List<StatInfo> childrenIfAny = listFiles(path);
            if (childrenIfAny != null && !childrenIfAny.isEmpty()) {
                log.info("目录元数据缺失，但检测到子项，按目录递归删除: {} ({} 个子项)", path, childrenIfAny.size());
                boolean ok = deleteDirectoryRecursively(path);
                if (ok) {
                    log.info("成功删除目录(按隐式目录处理): {}", path);
                }
                return ok;
            }
            log.warn("文件/目录不存在: {}", path);
            return false;
        }
        
        try {
            if (statInfo.getType() == FileType.Directory) {
                // 目录删除，先递归删除子文件
                log.info("开始递归删除目录: {}", path);
                boolean recursiveDeleteSuccess = deleteDirectoryRecursively(path);
                if (!recursiveDeleteSuccess) {
                    log.error("递归删除目录失败: {}", path);
                    return false;
                }
            } else {
                // 文件删除：先通知 DataServer 删除实际数据，再删除元数据
                List<ReplicaData> replicas = statInfo.getReplicaData();
                if (replicas != null && !replicas.isEmpty()) {
                    int deletedCount = dataServerClient.deleteFromMultipleDataServers(replicas);
                    log.info("删除文件: {}, 在DataServer上成功删除: {}/{}", path, deletedCount, replicas.size());
                } else {
                    log.info("删除文件: {}, 无副本信息，直接删除元数据", path);
                }
            }
            
            // 删除成功后，从RocksDB和内存缓存中删除元数据
            metadataStorage.deleteMetadata(path);
            log.info("成功删除文件/目录: {}", path);
            return true;
            
        } catch (Exception e) {
            log.error("删除文件/目录异常: {}", path, e);
            return false;
        }
    }
    
    /**
     * 递归删除目录及其所有内容
     */
    private boolean deleteDirectoryRecursively(String dirPath) {
        try {
            List<StatInfo> children = listFiles(dirPath);
            if (children.isEmpty()) {
                log.debug("目录为空，直接删除: {}", dirPath);
                // 删除空目录的元数据
                metadataStorage.deleteMetadata(dirPath);
                return true;
            }
            
            log.info("目录 {} 包含 {} 个子项，开始递归删除", 
                    dirPath, children.size());
            
            // 先删除所有子项
            for (StatInfo child : children) {
                String childPath = child.getPath();
                if (child.getType() == FileType.Directory) {
                    // 递归删除子目录
                    if (!deleteDirectoryRecursively(childPath)) {
                        log.error("递归删除子目录失败: {}", childPath);
                        return false;
                    }
                } else {
                    // 删除子文件，先调用DataServer删除实际数据
                    List<ReplicaData> replicas = child.getReplicaData();
                    if (replicas != null && !replicas.isEmpty()) {
                        int deletedCount = dataServerClient.deleteFromMultipleDataServers(replicas);
                        log.info("删除子文件: {}, 在DataServer上成功删除: {}/{}", 
                                childPath, deletedCount, replicas.size());
                    }
                }
                
                // 删除子项的元数据
                metadataStorage.deleteMetadata(childPath);
            }
            
            log.info("目录 {} 的所有子项删除完成", dirPath);
            return true;
            
        } catch (Exception e) {
            log.error("递归删除目录异常: {}", dirPath, e);
            return false;
        }
    }
    

    
    /**
     * 获取所有数据服务器
     */
    public List<Map<String, Object>> getDataServers() {
        Map<String, Map<String, Object>> allDataServers = zkDataServerService.getAllDataServers();
        List<Map<String, Object>> result = new ArrayList<>();
        
        for (Map<String, Object> server : allDataServers.values()) {
            // 确保所有必要的字段都存在
            Map<String, Object> serverInfo = new HashMap<>(server);
            
            // 确保容量字段存在
            if (!serverInfo.containsKey("totalCapacity")) {
                serverInfo.put("totalCapacity", 1024 * 1024 * 1024L); // 1GB 默认值
            }
            if (!serverInfo.containsKey("usedCapacity")) {
                serverInfo.put("usedCapacity", 0L);
            }
            if (!serverInfo.containsKey("remainingCapacity")) {
                long total = (Long) serverInfo.get("totalCapacity");
                long used = (Long) serverInfo.get("usedCapacity");
                serverInfo.put("remainingCapacity", Math.max(0, total - used));
            }
            
            result.add(serverInfo);
        }
        
        return result;
    }
    

    
    /**
     * 获取所有文件元数据
     */
    public List<StatInfo> getAllFiles() {
        return metadataStorage.getAllMetadata();
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
    public boolean fileExists(String path) {
        return metadataStorage.exists(path);
    }
    
    /**
     * 获取文件状态信息
     */
    public StatInfo getFileStatus(String path) {
        return getFile(path);
    }
    
    /**
     * 创建目录
     */
    public StatInfo createDirectory(String path) {
        return createFile(path, FileType.Directory);
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
    public Map<String, Object> getGlobalStats() {
        Map<String, Object> stats = new HashMap<>();
        List<StatInfo> allFiles = metadataStorage.getAllMetadata();
        stats.put("totalFiles", allFiles.size());
        stats.put("totalDirectories", (int) allFiles.stream()
                .filter(f -> f.getType() == FileType.Directory).count());
        stats.put("totalRegularFiles", (int) allFiles.stream()
                .filter(f -> f.getType() == FileType.File).count());
        long totalSize = allFiles.stream()
                .filter(f -> f.getType() == FileType.File)
                .mapToLong(StatInfo::getSize)
                .sum();
        stats.put("totalSize", totalSize);
        return stats;
    }
}
