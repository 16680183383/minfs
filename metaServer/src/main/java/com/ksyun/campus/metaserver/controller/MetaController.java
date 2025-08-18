package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.services.MetaService;
import com.ksyun.campus.metaserver.services.FsckServices;
import com.ksyun.campus.metaserver.services.MetadataStorageService;
import com.ksyun.campus.metaserver.services.ZkMetaServerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
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
    
    @Autowired
    private ZkMetaServerService zkMetaServerService;
    
    /**
     * 获取文件状态信息
     */
    @RequestMapping("stats")
    public ResponseEntity<StatInfo> stats(@RequestParam String path) {
        log.info("获取文件状态: path={}", path);
        StatInfo statInfo = metaService.getFile(path);
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
    public ResponseEntity<StatInfo> createFile(@RequestParam String path) {
        log.info("创建文件: path={}", path);
        StatInfo statInfo = metaService.createFile(path, FileType.File);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 创建目录
     */
    @RequestMapping("mkdir")
    public ResponseEntity<StatInfo> mkdir(@RequestParam String path) {
        log.info("创建目录: path={}", path);
        StatInfo statInfo = metaService.createDirectory(path);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 写入文件数据
     */
    @RequestMapping(value = "write", method = RequestMethod.POST)
    public ResponseEntity<StatInfo> writeFile(
            @RequestParam String path,
            @RequestParam int offset,
            @RequestParam int length,
            HttpServletRequest request) {
        try {
            log.info("写入文件: path={}, offset={}, length={}", 
                    path, offset, length);
            
            // 读取请求体中的二进制数据
            byte[] data = request.getInputStream().readAllBytes();
            
            // 调用MetaService写入文件
            StatInfo statInfo = metaService.writeFile(path, data, offset, length);
            
            return ResponseEntity.ok(statInfo);
            
        } catch (Exception e) {
            log.error("写入文件失败: path={}", path, e);
            return ResponseEntity.status(500).body(null);
        }
    }
    
    /**
     * 列出目录内容
     */
    @RequestMapping("listStatus")
    public ResponseEntity<List<StatInfo>> listdir(@RequestParam String path) {
        log.info("列出目录内容: path={}", path);
        List<StatInfo> files = metaService.listFiles(path);
        return ResponseEntity.ok(files);
    }
    
    /**
     * 删除文件/目录（支持递归删除非空目录）
     */
    @RequestMapping("delete")
    public ResponseEntity<Map<String, Object>> delete(@RequestParam String path) {
        log.info("删除文件/目录: path={}", path);
        
        boolean deleteSuccess = metaService.deleteFile(path);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", deleteSuccess);
        result.put("path", path);
        result.put("message", deleteSuccess ? "删除成功" : "删除失败");
        
        if (deleteSuccess) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.status(500).body(result);
        }
    }


    
    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     */
    @RequestMapping("open")
    public ResponseEntity<StatInfo> open(@RequestParam String path) {
        log.info("打开文件: path={}", path);
        StatInfo statInfo = metaService.getFile(path);
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
     * 手动触发FSCK检查
     */
    @RequestMapping("fsck/manual")
    public ResponseEntity<Map<String, Object>> manualFsck() {
        log.info("手动触发FSCK检查");
        fsckServices.manualFsck();
        Map<String, Object> results = fsckServices.getFsckResults();
        return ResponseEntity.ok(results);
    }
    
    /**
     * 获取FSCK检查结果
     */
    @RequestMapping("fsck/results")
    public ResponseEntity<Map<String, Object>> getFsckResults() {
        log.info("获取FSCK检查结果");
        Map<String, Object> results = fsckServices.getFsckResults();
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
    public ResponseEntity<Map<String, String>> getFileSystemList() {
        log.info("获取存储信息");
        Map<String, String> stats = metadataStorage.getStats();
        return ResponseEntity.ok(stats);
    }
    
    /**
     * 获取指定文件系统统计信息
     */
    @RequestMapping("filesystem/stats")
    public ResponseEntity<Map<String, Object>> getFileSystemStats() {
        log.info("获取全局统计信息");
        Map<String, Object> stats = metaService.getGlobalStats();
        return ResponseEntity.ok(stats);
    }
    
    /**
     * 检查文件是否存在
     */
    @RequestMapping("exists")
    public ResponseEntity<Map<String, Object>> fileExists(@RequestParam String path) {
        log.info("检查文件是否存在: path={}", path);
        boolean exists = metaService.fileExists(path);
        
        Map<String, Object> result = new HashMap<>();
        result.put("path", path);
        result.put("exists", exists);
        result.put("message", exists ? "文件存在" : "文件不存在");
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * 获取文件状态
     */
    @RequestMapping("getStatus")
    public ResponseEntity<StatInfo> getStatus(@RequestParam String path) {
        log.info("获取文件状态: path={}", path);
        StatInfo statInfo = metaService.getFileStatus(path);
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
    public ResponseEntity<StatInfo> createDirectory(@RequestParam String path) {
        log.info("创建目录: path={}", path);
        StatInfo statInfo = metaService.createDirectory(path);
        return ResponseEntity.ok(statInfo);
    }
    
    /**
     * 重命名文件或目录
     */
    @RequestMapping("rename")
    public ResponseEntity<Map<String, Object>> renameFile(@RequestParam String oldPath,
                                                        @RequestParam String newPath) {
        log.info("重命名文件/目录: oldPath={}, newPath={}", oldPath, newPath);
        
        boolean renameSuccess = metaService.renameFile(oldPath, newPath);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", renameSuccess);
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
    public ResponseEntity<List<StatInfo>> listAllFiles() {
        log.info("列出所有文件");
        List<StatInfo> allFiles = metaService.getAllFiles();
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
    
    /**
     * 检查ZK注册状态
     */
    @RequestMapping("zk/check")
    public ResponseEntity<Map<String, Object>> checkZkRegistration() {
        log.info("检查ZK注册状态");
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 检查当前MetaServer信息
            Map<String, Object> currentInfo = zkMetaServerService.getCurrentMetaServerInfo();
            result.put("currentMetaServer", currentInfo);
            
            // 检查是否为Leader
            boolean isLeader = zkMetaServerService.isLeader();
            result.put("isLeader", isLeader);
            
            // 检查ZK连接状态
            boolean zkConnected = zkMetaServerService.isZkConnected();
            result.put("zkConnected", zkConnected);
            
            // 获取所有MetaServer节点
            List<Map<String, Object>> allMetaServers = zkMetaServerService.getAllMetaServers();
            result.put("allMetaServers", allMetaServers);
            result.put("totalMetaServers", allMetaServers.size());
            
            result.put("status", "success");
            result.put("timestamp", System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("检查ZK注册状态失败", e);
            result.put("status", "error");
            result.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * 获取ZK集群状态
     */
    @RequestMapping("zk/cluster")
    public ResponseEntity<Map<String, Object>> getZkClusterStatus() {
        log.info("获取ZK集群状态");
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 获取所有MetaServer节点
            List<Map<String, Object>> allMetaServers = zkMetaServerService.getAllMetaServers();
            
            // 统计集群状态
            long activeCount = allMetaServers.stream()
                .filter(server -> "active".equals(server.get("status")))
                .count();
            
            result.put("totalMetaServers", allMetaServers.size());
            result.put("activeMetaServers", activeCount);
            result.put("inactiveMetaServers", allMetaServers.size() - activeCount);
            result.put("metaServers", allMetaServers);
            
            // 检查当前节点状态
            Map<String, Object> currentInfo = zkMetaServerService.getCurrentMetaServerInfo();
            result.put("currentNode", currentInfo);
            
            result.put("status", "success");
            result.put("timestamp", System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("获取ZK集群状态失败", e);
            result.put("status", "error");
            result.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }
}
