package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.services.MetaService;
import com.ksyun.campus.metaserver.services.FsckServices;
import com.ksyun.campus.metaserver.services.MetadataStorageService;
import com.ksyun.campus.metaserver.services.ZkMetaServerService;
import com.ksyun.campus.metaserver.services.ReplicationService;
import com.ksyun.campus.metaserver.domain.ReplicationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.services.ZkDataServerService;

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
    
    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private org.springframework.web.client.RestTemplate restTemplate;

    @Autowired
    private ZkDataServerService zkDataServerService;

    private java.net.URI buildLeaderUri(String leader, String path, String query) {
        try {
            return new java.net.URI("http://" + leader + path + (query == null ? "" : ("?" + query)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 获取文件状态信息
     */
    @RequestMapping("stats")
    public ResponseEntity<StatInfo> stats(@RequestParam String path) {
        try {
            // 参数验证
            if (path == null || path.trim().isEmpty()) {
                log.warn("获取文件状态失败: 路径为空");
                return ResponseEntity.badRequest().build();
            }
            if (!path.startsWith("/")) {
                log.warn("获取文件状态失败: 路径格式错误: {}", path);
                return ResponseEntity.badRequest().build();
            }
            
            log.info("获取文件状态: path={}", path);
            StatInfo statInfo = metaService.getFile(path);
            if (statInfo != null) {
                return ResponseEntity.ok(statInfo);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (IllegalArgumentException e) {
            log.warn("获取文件状态失败: 参数错误: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("获取文件状态失败: path={}", path, e);
            return ResponseEntity.status(500).build();
        }
    }
    
    /**
     * 创建文件
     */
    @RequestMapping("create")
    public ResponseEntity<StatInfo> createFile(@RequestParam String path) {
        try {
            // 参数验证
            if (path == null || path.trim().isEmpty()) {
                log.warn("创建文件失败: 路径为空");
                return ResponseEntity.badRequest().build();
            }
            if (!path.startsWith("/")) {
                log.warn("创建文件失败: 路径格式错误: {}", path);
                return ResponseEntity.badRequest().build();
            }
            if (path.endsWith("/")) {
                log.warn("创建文件失败: 文件路径不能以/结尾: {}", path);
                return ResponseEntity.badRequest().build();
            }
            
            log.info("创建文件: path={}", path);
            if (!zkMetaServerService.isLeader()) {
                String leader = zkMetaServerService.getLeaderAddress();
                if (leader != null) {
                    java.net.URI uri = buildLeaderUri(leader, "/create", "path=" + path);
                    ResponseEntity<StatInfo> resp = restTemplate.exchange(uri, org.springframework.http.HttpMethod.GET, null, StatInfo.class);
                    return resp;
                } else {
                    log.error("创建文件失败: 无法获取Leader地址");
                    return ResponseEntity.status(503).build();
                }
            }
            
            StatInfo statInfo = metaService.createFile(path, FileType.File);
            // 先确保父目录链在Follower存在（幂等）
            try {
                String parent = path;
                java.util.List<String> dirChain = new java.util.ArrayList<>();
                int idx = parent.lastIndexOf('/');
                if (idx > 0) {
                    parent = parent.substring(0, idx);
                    while (parent != null && !"/".equals(parent) && parent.startsWith("/")) {
                        dirChain.add(0, parent);
                        int i = parent.lastIndexOf('/');
                        if (i <= 0) break;
                        parent = parent.substring(0, i);
                    }
                }
                for (String dir : dirChain) {
                    replicationService.replicateToFollowers(ReplicationType.CREATE_DIR, dir, java.util.Map.of());
                }
            } catch (Exception e) {
                log.warn("复制父目录链失败(不影响本地创建): {}", path, e);
            }
            replicationService.replicateToFollowers(ReplicationType.CREATE_FILE, path, java.util.Map.of("size", 0));
            return ResponseEntity.ok(statInfo);
            
        } catch (IllegalArgumentException e) {
            log.warn("创建文件失败: 参数错误: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("创建文件失败: path={}", path, e);
            return ResponseEntity.status(500).build();
        }
    }
    
    /**
     * 创建目录
     */
    @RequestMapping("mkdir")
    public ResponseEntity<StatInfo> mkdir(@RequestParam String path) {
        try {
            // 参数验证
            if (path == null || path.trim().isEmpty()) {
                log.warn("创建目录失败: 路径为空");
                return ResponseEntity.badRequest().build();
            }
            if (!path.startsWith("/")) {
                log.warn("创建目录失败: 路径格式错误: {}", path);
                return ResponseEntity.badRequest().build();
            }
            if (path.endsWith("/")) {
                log.warn("创建目录失败: 目录路径不能以/结尾: {}", path);
                return ResponseEntity.badRequest().build();
            }
            
            log.info("创建目录: path={}", path);
            
            if (!zkMetaServerService.isLeader()) {
                String leader = zkMetaServerService.getLeaderAddress();
                if (leader != null) {
                    java.net.URI uri = buildLeaderUri(leader, "/mkdir", "path=" + path);
                    ResponseEntity<StatInfo> resp = restTemplate.exchange(uri, org.springframework.http.HttpMethod.GET, null, StatInfo.class);
                    return resp;
                } else {
                    log.error("创建目录失败: 无法获取Leader地址");
                    return ResponseEntity.status(503).build();
                }
            }
            
            StatInfo statInfo = metaService.createDirectory(path);
            
            // 为保证Follower侧的父目录链也存在，这里将父链全部复制（幂等）
            try {
                String p = path;
                java.util.List<String> chain = new java.util.ArrayList<>();
                while (p != null && !"/".equals(p) && p.startsWith("/")) {
                    chain.add(0, p); // 头插，最终从上到下
                    int idx = p.lastIndexOf('/');
                    if (idx <= 0) {
                        break;
                    }
                    p = p.substring(0, idx);
                }
                // 复制父链每一层
                for (String dir : chain) {
                    replicationService.replicateToFollowers(ReplicationType.CREATE_DIR, dir, Map.of());
                }
            } catch (Exception e) {
                log.warn("复制父目录链失败(不影响本地创建): {}", path, e);
                // 回退为只复制当前目录
                replicationService.replicateToFollowers(ReplicationType.CREATE_DIR, path, Map.of());
            }
            return ResponseEntity.ok(statInfo);
            
        } catch (IllegalArgumentException e) {
            log.warn("创建目录失败: 参数错误: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("创建目录失败: path={}", path, e);
            return ResponseEntity.status(500).build();
        }
    }
    
    /**
     * 读取文件数据
     * 支持分块读取，返回文件内容
     */
    @RequestMapping(value = "read", method = RequestMethod.GET)
    public ResponseEntity<byte[]> readFile(
            @RequestParam String path,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "-1") int length) {
        try {
            // 参数验证
            if (path == null || path.trim().isEmpty()) {
                log.warn("读取文件失败: 路径为空");
                return ResponseEntity.badRequest().build();
            }
            if (!path.startsWith("/")) {
                log.warn("读取文件失败: 路径格式错误: {}", path);
                return ResponseEntity.badRequest().build();
            }
            if (offset < 0) {
                log.warn("读取文件失败: 偏移量不能为负数: {}", offset);
                return ResponseEntity.badRequest().build();
            }
            if (length < -1) {
                log.warn("读取文件失败: 长度不能小于-1: {}", length);
                return ResponseEntity.badRequest().build();
            }
            
            log.info("读取文件: path={}, offset={}, length={}", path, offset, length);
            
            // 如果不是Leader，则将读请求转发给Leader
            if (!zkMetaServerService.isLeader()) {
                String leader = zkMetaServerService.getLeaderAddress();
                if (leader != null) {
                    java.net.URI uri = buildLeaderUri(leader, "/read", "path=" + path + "&offset=" + offset + "&length=" + length);
                    ResponseEntity<byte[]> resp = restTemplate.exchange(uri, org.springframework.http.HttpMethod.GET, null, byte[].class);
                    return resp;
                } else {
                    log.error("读取文件失败: 无法获取Leader地址");
                    return ResponseEntity.status(503).build();
                }
            }
            
            // 调用MetaService读取文件
            byte[] data = metaService.readFile(path, offset, length);
            if (data != null) {
                return ResponseEntity.ok(data);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (IllegalArgumentException e) {
            log.warn("读取文件失败: 参数错误: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("读取文件失败: path={}", path, e);
            return ResponseEntity.status(500).body(("读取文件失败: " + e.getMessage()).getBytes());
        }
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
            
            // 如果不是Leader，则将写请求和原始数据转发给Leader
            if (!zkMetaServerService.isLeader()) {
                String leader = zkMetaServerService.getLeaderAddress();
                if (leader != null) {
                    java.net.URI uri = buildLeaderUri(leader, "/write", "path=" + path + "&offset=" + offset + "&length=" + length);
                    org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
                    headers.setContentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM);
                    byte[] forwardBody = request.getInputStream().readAllBytes();
                    org.springframework.http.HttpEntity<byte[]> entity = new org.springframework.http.HttpEntity<>(forwardBody, headers);
                    ResponseEntity<StatInfo> resp = restTemplate.exchange(uri, org.springframework.http.HttpMethod.POST, entity, StatInfo.class);
                    return resp;
                } else {
                    return ResponseEntity.status(503).body(null);
                }
            }

            // 读取请求体中的二进制数据（Leader本地写）
            byte[] data = request.getInputStream().readAllBytes();
            
            // 调用MetaService写入文件
            StatInfo statInfo = metaService.writeFile(path, data, offset, length);

            // 先复制父目录链与CREATE_FILE（幂等，避免边写边建时Follower缺失父链/文件）
            try {
                String parent = path;
                java.util.List<String> dirChain = new java.util.ArrayList<>();
                int idx = parent.lastIndexOf('/');
                if (idx > 0) {
                    parent = parent.substring(0, idx);
                    while (parent != null && !"/".equals(parent) && parent.startsWith("/")) {
                        dirChain.add(0, parent);
                        int i = parent.lastIndexOf('/');
                        if (i <= 0) break;
                        parent = parent.substring(0, i);
                    }
                }
                for (String dir : dirChain) {
                    replicationService.replicateToFollowers(ReplicationType.CREATE_DIR, dir, java.util.Map.of());
                }
                replicationService.replicateToFollowers(ReplicationType.CREATE_FILE, path, java.util.Map.of("size", statInfo.getSize()));
            } catch (Exception ex) {
                log.warn("写入前置复制父链/文件失败(不影响本地写): {}", path, ex);
            }

            // 复制到从节点：携带大小与副本信息（可选）
            java.util.List<java.util.Map<String, Object>> replicasPayload = new java.util.ArrayList<>();
            if (statInfo.getReplicaData() != null) {
                for (com.ksyun.campus.metaserver.domain.ReplicaData r : statInfo.getReplicaData()) {
                    java.util.Map<String, Object> m = new java.util.HashMap<>();
                    m.put("id", r.id);
                    m.put("dsNode", r.dsNode);
                    m.put("path", r.path);
                    m.put("offset", r.offset);
                    m.put("length", r.length);
                    m.put("isPrimary", r.isPrimary);
                    replicasPayload.add(m);
                }
            }
            java.util.Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("size", statInfo.getSize());
            if (!replicasPayload.isEmpty()) {
                payload.put("replicas", replicasPayload);
            }
            replicationService.replicateToFollowers(ReplicationType.WRITE, path, payload);
            
            return ResponseEntity.ok(statInfo);
            
        } catch (Exception e) {
            log.error("写入文件失败: path={}", path, e);
            return ResponseEntity.status(500).body(null);
        }
    }
    
    /**
     * 列出目录内容
     */
    @RequestMapping("listdir")
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
        // 非Leader将请求转发到Leader，避免各自执行导致不一致
        if (!zkMetaServerService.isLeader()) {
            String leader = zkMetaServerService.getLeaderAddress();
            if (leader != null) {
                java.net.URI uri = buildLeaderUri(leader, "/delete", "path=" + path);
                ResponseEntity<Map> resp = restTemplate.exchange(uri, org.springframework.http.HttpMethod.GET, null, Map.class);
                @SuppressWarnings("unchecked")
                Map<String, Object> body = resp.getBody() == null ? new HashMap<>() : (Map<String, Object>) resp.getBody();
                return ResponseEntity.status(resp.getStatusCode()).body(body);
            }
        }
        
        boolean deleteSuccess = metaService.deleteFile(path);
        if (deleteSuccess) {
            replicationService.replicateToFollowers(ReplicationType.DELETE, path, Map.of());
        }
        
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
        try {
            // 1. 获取MetaServer集群信息
            Map<String, Object> metaServerInfo = new HashMap<>();
            metaServerInfo.put("followerAddresses", zkMetaServerService.getFollowerAddresses());
            metaServerInfo.put("leaderAddress", zkMetaServerService.getLeaderAddress());
            clusterInfo.put("metaServers", metaServerInfo);

            // 2. 获取DataServer集群信息
            List<Map<String, Object>> dataServers = metaService.getDataServers();

            clusterInfo.put("dataServers", dataServers);
            clusterInfo.put("totalDataServers", dataServers.size());
            clusterInfo.put("activeDataServers", dataServers.stream()
                    .mapToInt(server -> {
                        Object activeObj = server.get("active");
                        if (activeObj instanceof Boolean) {
                            return (Boolean) activeObj ? 1 : 0;
                        }
                        return 0;
                    }).sum());

            // 3. 获取主副本分布统计
            Map<String, Object> replicaDistribution = getPrimaryReplicaDistribution();
            clusterInfo.put("replicaDistribution", replicaDistribution);

            // 4. 获取集群健康状态
            Map<String, Object> healthStatus = new HashMap<>();
            healthStatus.put("metaServerHealthy", true); // MetaServer 总是健康的
            healthStatus.put("dataServerHealthy", dataServers.stream().anyMatch(server -> (Boolean) server.get("active")));
            healthStatus.put("overallHealth", "HEALTHY");
            clusterInfo.put("healthStatus", healthStatus);
        } catch (Exception e) {
            log.error("获取集群信息失败", e);
            clusterInfo.put("error", "获取集群信息失败: " + e.getMessage());
        }
        return ResponseEntity.ok(clusterInfo);
    }
    
    /**
     * 获取主副本分布统计
     */
    private Map<String, Object> getPrimaryReplicaDistribution() {
        Map<String, Object> distribution = new HashMap<>();
        
        try {
            // 获取所有文件元数据
            List<StatInfo> allFiles = metaService.getAllFiles();
            
            // 统计每个DataServer上的主副本数量
            Map<String, Integer> primaryReplicaCount = new HashMap<>();
            Map<String, Integer> totalReplicaCount = new HashMap<>();
            
            for (StatInfo fileInfo : allFiles) {
                if (fileInfo.getType() == FileType.File && fileInfo.getReplicaData() != null) {
                    for (ReplicaData replica : fileInfo.getReplicaData()) {
                        String dsNode = replica.dsNode;
                        
                        // 统计总副本数
                        totalReplicaCount.put(dsNode, totalReplicaCount.getOrDefault(dsNode, 0) + 1);
                        
                        // 统计主副本数
                        if (replica.isPrimary) {
                            primaryReplicaCount.put(dsNode, primaryReplicaCount.getOrDefault(dsNode, 0) + 1);
                        }
                    }
                }
            }
            
            distribution.put("primaryReplicaCount", primaryReplicaCount);
            distribution.put("totalReplicaCount", totalReplicaCount);
            distribution.put("totalFiles", allFiles.stream()
                    .filter(f -> f.getType() == FileType.File)
                    .count());
            distribution.put("totalDirectories", allFiles.stream()
                    .filter(f -> f.getType() == FileType.Directory)
                    .count());
            
            log.info("主副本分布统计完成: {} 个文件, {} 个目录", 
                    distribution.get("totalFiles"), distribution.get("totalDirectories"));
            
        } catch (Exception e) {
            log.error("获取主副本分布统计失败", e);
            distribution.put("error", "获取主副本分布统计失败: " + e.getMessage());
        }
        
        return distribution;
    }
    
    /**
     * 获取主副本分布统计
     * 专门用于查看当前集群节点上的主副本分布情况
     */
    @RequestMapping("replica/distribution")
    public ResponseEntity<Map<String, Object>> getReplicaDistribution() {
        log.info("获取主副本分布统计");
        try {
            Map<String, Object> distribution = getPrimaryReplicaDistribution();
            return ResponseEntity.ok(distribution);
        } catch (Exception e) {
            log.error("获取主副本分布统计失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "获取主副本分布统计失败: " + e.getMessage());
            return ResponseEntity.status(500).body(error);
        }
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

    // Leader导出快照，Follower用于重放
    @RequestMapping(value = "internal/snapshot", method = RequestMethod.GET)
    public ResponseEntity<Map<String, Object>> exportSnapshot() {
        List<StatInfo> all = metaService.getAllFiles();
        List<Map<String, Object>> files = all.stream().map(s -> {
            Map<String, Object> m = new HashMap<>();
            m.put("path", s.getPath());
            m.put("type", s.getType().name());
            m.put("size", s.getSize());
            if (s.getReplicaData() != null && !s.getReplicaData().isEmpty()) {
                List<Map<String, Object>> replicas = s.getReplicaData().stream().map(r -> {
                    Map<String, Object> rm = new HashMap<>();
                    rm.put("id", r.id);
                    rm.put("dsNode", r.dsNode);
                    rm.put("path", r.path);
                    rm.put("offset", r.offset);
                    rm.put("length", r.length);
                    rm.put("isPrimary", r.isPrimary);
                    return rm;
                }).toList();
                m.put("replicas", replicas);
            }
            return m;
        }).toList();
        return ResponseEntity.ok(Map.of("files", files));
    }

    // 内部复制接收接口（仅Follower调用）
    @RequestMapping(value = "internal/replicate", method = RequestMethod.POST)
    public ResponseEntity<Map<String, Object>> internalReplicate(
            @RequestParam String type,
            @RequestParam String path,
            @RequestBody(required = false) Map<String, Object> payload) {
        try {
            boolean ok = replicationService.applyReplication(ReplicationType.valueOf(type), path, payload == null ? Map.of() : payload);
            return ResponseEntity.ok(Map.of("success", ok));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    // 手动触发从Leader追赶一次（仅Follower有效）
    @RequestMapping(value = "internal/catchup", method = RequestMethod.POST)
    public ResponseEntity<Map<String, Object>> manualCatchup() {
        try {
            replicationService.catchUpFromLeaderIfNeeded();
            return ResponseEntity.ok(Map.of("success", true));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("success", false, "error", e.getMessage()));
        }
    }
}
