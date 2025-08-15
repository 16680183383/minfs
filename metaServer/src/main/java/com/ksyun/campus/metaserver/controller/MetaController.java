package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.services.MetaService;
import com.ksyun.campus.metaserver.services.FsckServices;
import com.ksyun.campus.metaserver.services.MetadataStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/")
public class MetaController {
    
    @Autowired
    private MetaService metaService;
    
    @Autowired
    private FsckServices fsckServices;
    
    @Autowired
    private MetadataStorageService metadataStorage;
    
    /**
     * 获取文件状态信息
     */
    @RequestMapping("stats")
    public ResponseEntity<StatInfo> stats(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("获取文件状态: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.getFile(fileSystemName, path);
        if (statInfo != null) {
            return ResponseEntity.ok(statInfo);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * 创建文件
     */
    @RequestMapping("create")
    public ResponseEntity<StatInfo> createFile(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("创建文件: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.createFile(fileSystemName, path, FileType.File);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 创建目录
     */
    @RequestMapping("mkdir")
    public ResponseEntity<StatInfo> mkdir(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("创建目录: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.createDirectory(fileSystemName, path);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 列出目录内容
     */
    @RequestMapping("listStatus")
    public ResponseEntity<List<StatInfo>> listdir(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("列出目录内容: fileSystemName={}, path={}", fileSystemName, path);
        List<StatInfo> files = metaService.listFiles(fileSystemName, path);
        return ResponseEntity.ok(files);
    }
    
    /**
     * 删除文件/目录（支持递归删除非空目录）
     */
    @RequestMapping("delete")
    public ResponseEntity<Map<String, Object>> delete(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("删除文件/目录: fileSystemName={}, path={}", fileSystemName, path);
        
        boolean deleteSuccess = metaService.deleteFile(fileSystemName, path);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", deleteSuccess);
        result.put("fileSystemName", fileSystemName);
        result.put("path", path);
        result.put("message", deleteSuccess ? "删除成功" : "删除失败");
        
        if (deleteSuccess) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.status(500).body(result);
        }
    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     */
    @RequestMapping("write")
    public ResponseEntity<String> commitWrite(@RequestHeader String fileSystemName, 
                                           @RequestParam String path, 
                                           @RequestParam int offset, 
                                           @RequestParam int length) {
        log.info("提交写入: fileSystemName={}, path={}, offset={}, length={}", fileSystemName, path, offset, length);
        metaService.updateFileSize(fileSystemName, path, offset + length);
        return ResponseEntity.ok("写入成功");
    }
    
    /**
     * 直接写入文件数据到DataServer（三副本同步）
     */
    @RequestMapping("writeData")
    public ResponseEntity<Map<String, Object>> writeFileData(@RequestHeader String fileSystemName,
                                                           @RequestParam String path,
                                                           @RequestParam int offset,
                                                           @RequestParam int length,
                                                           @RequestBody byte[] data) {
        log.info("直接写入文件数据: fileSystemName={}, path={}, offset={}, length={}, dataSize={}", 
                fileSystemName, path, offset, length, data.length);
        
        int successCount = metaService.writeFileData(fileSystemName, path, data, offset, length);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", successCount >= 2);
        result.put("fileSystemName", fileSystemName);
        result.put("successReplicas", successCount);
        result.put("totalReplicas", 3);
        result.put("message", successCount >= 2 ? "写入成功" : "写入失败");
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * 从DataServer读取文件数据
     */
    @RequestMapping("readData")
    public ResponseEntity<byte[]> readFileData(@RequestHeader String fileSystemName,
                                             @RequestParam String path,
                                             @RequestParam int offset,
                                             @RequestParam int length) {
        log.info("读取文件数据: fileSystemName={}, path={}, offset={}, length={}", fileSystemName, path, offset, length);
        
        byte[] data = metaService.readFileData(fileSystemName, path, offset, length);
        if (data != null) {
            return ResponseEntity.ok(data);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * 删除文件数据（从所有DataServer删除）
     */
    @RequestMapping("deleteData")
    public ResponseEntity<Map<String, Object>> deleteFileData(@RequestHeader String fileSystemName,
                                                            @RequestParam String path) {
        log.info("删除文件数据: fileSystemName={}, path={}", fileSystemName, path);
        
        int deletedCount = metaService.deleteFileData(fileSystemName, path);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", deletedCount > 0);
        result.put("fileSystemName", fileSystemName);
        result.put("deletedReplicas", deletedCount);
        result.put("message", "成功删除 " + deletedCount + " 个副本");
        
        return ResponseEntity.ok(result);
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     */
    @RequestMapping("open")
    public ResponseEntity<StatInfo> open(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("打开文件: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.getFile(fileSystemName, path);
        if (statInfo != null && statInfo.getType() == FileType.File) {
            return ResponseEntity.ok(statInfo);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * 获取集群信息
     */
    @RequestMapping("cluster/info")
    public ResponseEntity<Map<String, Object>> getClusterInfo() {
        log.info("获取集群信息");
        Map<String, Object> clusterInfo = new HashMap<>();
        
        // 获取数据服务器信息
        List<Map<String, Object>> dataServers = metaService.getDataServers().stream()
                .map(server -> {
                    Map<String, Object> serverInfo = new HashMap<>();
                    // 从Map中获取服务器信息
                    @SuppressWarnings("unchecked")
                    Map<String, Object> serverMap = (Map<String, Object>) server;
                    serverInfo.put("id", serverMap.get("id"));
                    serverInfo.put("address", serverMap.get("address"));
                    serverInfo.put("capacity", serverMap.get("capacity"));
                    serverInfo.put("usedSpace", serverMap.get("usedSpace"));
                    serverInfo.put("active", serverMap.get("active"));
                    return serverInfo;
                })
                .toList();
        
        clusterInfo.put("dataServers", dataServers);
        clusterInfo.put("totalDataServers", dataServers.size());
        clusterInfo.put("activeDataServers", dataServers.stream()
                .mapToInt(server -> (Boolean) server.get("active") ? 1 : 0)
                .sum());
        
        return ResponseEntity.ok(clusterInfo);
    }
    
    /**
     * 获取副本分布信息
     */
    @RequestMapping("cluster/replica-distribution")
    public ResponseEntity<Map<String, Integer>> getReplicaDistribution(@RequestHeader String fileSystemName) {
        log.info("获取副本分布信息: fileSystemName={}", fileSystemName);
        Map<String, Integer> distribution = metaService.getReplicaDistribution(fileSystemName);
        return ResponseEntity.ok(distribution);
    }
    
    /**
     * 手动触发FSCK检查
     */
    @RequestMapping("fsck/manual")
    public ResponseEntity<Map<String, Object>> manualFsck(@RequestHeader String fileSystemName) {
        log.info("手动触发FSCK检查: fileSystemName={}", fileSystemName);
        fsckServices.manualFsck(fileSystemName);
        Map<String, Object> results = fsckServices.getFsckResults(fileSystemName);
        return ResponseEntity.ok(results);
    }
    
    /**
     * 获取FSCK检查结果
     */
    @RequestMapping("fsck/results")
    public ResponseEntity<Map<String, Object>> getFsckResults(@RequestHeader String fileSystemName) {
        log.info("获取FSCK检查结果: fileSystemName={}", fileSystemName);
        Map<String, Object> results = fsckServices.getFsckResults(fileSystemName);
        return ResponseEntity.ok(results);
    }
    
    /**
     * 获取元数据存储状态
     */
    @RequestMapping("metadata/stats")
    public ResponseEntity<Map<String, Object>> getMetadataStats() {
        log.info("获取元数据存储状态");
        Map<String, String> stats = metadataStorage.getStats();
        // 转换为Map<String, Object>以匹配返回类型
        Map<String, Object> result = new HashMap<>(stats);
        return ResponseEntity.ok(result);
    }
    
    /**
     * 获取文件系统列表
     */
    @RequestMapping("filesystems")
    public ResponseEntity<List<String>> getFileSystemList() {
        log.info("获取文件系统列表");
        List<String> fileSystems = metadataStorage.getFileSystemList();
        return ResponseEntity.ok(fileSystems);
    }
    
    /**
     * 获取指定文件系统统计信息
     */
    @RequestMapping("filesystem/stats")
    public ResponseEntity<Map<String, Object>> getFileSystemStats(@RequestHeader String fileSystemName) {
        log.info("获取文件系统统计信息: fileSystemName={}", fileSystemName);
        Map<String, Object> stats = metadataStorage.getFileSystemStats(fileSystemName);
        return ResponseEntity.ok(stats);
    }
    
    /**
     * 检查文件是否存在
     */
    @RequestMapping("exists")
    public ResponseEntity<Map<String, Object>> fileExists(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("检查文件是否存在: fileSystemName={}, path={}", fileSystemName, path);
        boolean exists = metaService.fileExists(fileSystemName, path);
        
        Map<String, Object> result = new HashMap<>();
        result.put("fileSystemName", fileSystemName);
        result.put("path", path);
        result.put("exists", exists);
        result.put("message", exists ? "文件存在" : "文件不存在");
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * 获取文件状态
     */
    @RequestMapping("getStatus")
    public ResponseEntity<StatInfo> getStatus(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("获取文件状态: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.getFileStatus(fileSystemName, path);
        if (statInfo != null) {
            return ResponseEntity.ok(statInfo);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * 创建目录
     */
    @RequestMapping("createDirectory")
    public ResponseEntity<StatInfo> createDirectory(@RequestHeader String fileSystemName, @RequestParam String path) {
        log.info("创建目录: fileSystemName={}, path={}", fileSystemName, path);
        StatInfo statInfo = metaService.createDirectory(fileSystemName, path);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 重命名文件或目录
     */
    @RequestMapping("rename")
    public ResponseEntity<Map<String, Object>> renameFile(@RequestHeader String fileSystemName,
                                                        @RequestParam String oldPath,
                                                        @RequestParam String newPath) {
        log.info("重命名文件/目录: fileSystemName={}, oldPath={}, newPath={}", fileSystemName, oldPath, newPath);
        
        boolean renameSuccess = metaService.renameFile(fileSystemName, oldPath, newPath);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", renameSuccess);
        result.put("fileSystemName", fileSystemName);
        result.put("oldPath", oldPath);
        result.put("newPath", newPath);
        result.put("message", renameSuccess ? "重命名成功" : "重命名失败");
        
        if (renameSuccess) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.status(500).body(result);
        }
    }
    
    /**
     * 列出所有文件
     */
    @RequestMapping("listAll")
    public ResponseEntity<List<StatInfo>> listAllFiles(@RequestHeader String fileSystemName) {
        log.info("列出所有文件: fileSystemName={}", fileSystemName);
        List<StatInfo> allFiles = metaService.getAllFiles(fileSystemName);
        return ResponseEntity.ok(allFiles);
    }
    
    /**
     * 健康检查
     */
    @RequestMapping("health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        log.info("健康检查");
        Map<String, Object> health = new HashMap<>();
        health.put("status", "healthy");
        health.put("timestamp", System.currentTimeMillis());
        health.put("service", "MetaServer");
        
        // 获取DataServer状态
        Map<String, Object> dataServerStatus = metaService.getDataServerStatus();
        health.put("dataServerStatus", dataServerStatus);
        
        return ResponseEntity.ok(health);
    }
}
