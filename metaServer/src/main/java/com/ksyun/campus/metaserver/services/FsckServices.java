package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.ReplicaData;
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
    
    @Autowired
    private DataServerClientService dataServerClientService;
    
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
            // 针对每个文件系统分别检查，确保复制时带上正确的fileSystemName
            List<StatInfo> allFiles = new ArrayList<>();
            if (metadataStorage != null) {
                for (String fsName : metadataStorage.getAllFileSystemNames()) {
                    try {
                        List<StatInfo> list = metaService.getAllFiles(fsName);
                        if (list != null) {
                            // 在StatInfo上临时附加文件系统名（通过路径前缀或线程上下文传递不便，这里先走并行列表检查时携带fsName）
                            // 直接合并列表；verifyAndHealReplicas内会再次根据当前循环的fsName调用
                            allFiles.addAll(list);
                        }
                    } catch (Exception ignored) {}
                }
            } else {
                allFiles = metaService.getAllFiles();
            }
            int totalFiles = allFiles.size();
            int orphanedFiles = 0;
            int repairedFiles = 0;
            
            log.info("开始检查所有文件，共 {} 个", totalFiles);
            
            // 检查孤儿文件（元数据存在但副本信息不完整）
            for (StatInfo statInfo : allFiles) {
                if (statInfo.getType() == FileType.File) {
                    if (checkOrphanedFile(statInfo)) {
                        orphanedFiles++;
                    }
                    try {
                        boolean repaired = verifyAndHealReplicas(statInfo);
                        if (repaired) {
                            repairedFiles++;
                        }
                    } catch (Exception e) {
                        log.warn("文件副本校验/自愈失败: {}", statInfo.getPath(), e);
                    }
                }
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            results.put("totalFiles", totalFiles);
            results.put("orphanedFiles", orphanedFiles);
            results.put("repairedFiles", repairedFiles);
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
     * 校验文件各副本是否仍存在于对应的 DataServer；若不足3副本则触发自愈复制。
     * 返回是否进行了修复（有副本新增或元数据更新）。
     */
    private boolean verifyAndHealReplicas(StatInfo statInfo) {
        boolean changed = false;
        // 确定文件所属文件系统名称
        String fileSystemName = null;
        for (String fsName : metadataStorage.getAllFileSystemNames()) {
            try {
                if (metadataStorage.exists(fsName, statInfo.getPath())) {
                    fileSystemName = fsName;
                    break;
                }
            } catch (Exception ignored) {}
        }
        if (fileSystemName == null) {
            log.warn("无法确定文件所属的文件系统，跳过校验与自愈: {}", statInfo.getPath());
            return false;
        }
        List<ReplicaData> replicas = statInfo.getReplicaData();
        if (replicas == null) {
            replicas = new ArrayList<>();
        }
        // 1) 逐个向对应 DataServer 调用 checkFileExists
        List<ReplicaData> validReplicas = new ArrayList<>();
        for (ReplicaData r : replicas) {
            String ds = r.dsNode; // 形如 ip:port
            boolean exists = false;
            try {
                exists = dataServerClientService.checkFileExistsOnDataServer(ds, fileSystemName, statInfo.getPath());
            } catch (Exception e) {
                log.warn("副本健康检查失败：{} -> {}", ds, statInfo.getPath(), e);
            }
            if (exists) {
                validReplicas.add(r);
            } else {
                changed = true; // 失效副本将被剔除
                log.warn("检测到失效副本: {} -> {}，将从元数据中剔除", ds, statInfo.getPath());
            }
        }

        // 2) 若有效副本不足3，触发自愈复制
        int need = 3 - validReplicas.size();
        if (need > 0) {
            // 选择源副本（优先选择主副本；若无主副本，则任选一个有效副本）
            ReplicaData source = validReplicas.stream()
                    .filter(r -> r.isPrimary)
                    .findFirst()
                    .orElse(validReplicas.isEmpty() ? null : validReplicas.get(0));
            if (source == null) {
                log.warn("文件无可用源副本，无法自愈: {}", statInfo.getPath());
            } else {
                Set<String> used = new HashSet<>();
                for (ReplicaData r : validReplicas) {
                    used.add(r.dsNode);
                }
                List<String> targets = chooseTargetDataServers(used, need);
                for (String target : targets) {
                    try {
                        boolean ok = dataServerClientService.replicateBetweenDataServers(source.dsNode, target, fileSystemName, statInfo.getPath());
                        if (ok) {
                            ReplicaData nr = new ReplicaData();
                            nr.id = java.util.UUID.randomUUID().toString();
                            nr.dsNode = target;
                            nr.path = statInfo.getPath();
                            nr.offset = 0;
                            nr.length = statInfo.getSize();
                            nr.isPrimary = false;
                            validReplicas.add(nr);
                            changed = true;
                            log.info("自愈复制成功: {} -> {} ({})", source.dsNode, target, statInfo.getPath());
                        } else {
                            log.warn("自愈复制失败: {} -> {} ({})", source.dsNode, target, statInfo.getPath());
                        }
                    } catch (Exception e) {
                        log.warn("自愈复制异常: {} -> {} ({})", source.dsNode, target, statInfo.getPath(), e);
                    }
                }
            }
        }

        // 3) 若有效副本超过3，执行过副本清理（删除多余副本并更新元数据）
        if (validReplicas.size() > 3) {
            boolean removed = cleanupOverReplicas(statInfo, validReplicas);
            changed = changed || removed;
        }

        // 4) 清理历史脏数据：在未分配该文件副本的DataServer上，如果仍残留数据则删除
        int staleDeleted = cleanupStaleReplicasNotInMetadata(fileSystemName, statInfo, validReplicas);
        if (staleDeleted > 0) {
            log.info("清理历史脏副本: {} 个 ({})", staleDeleted, statInfo.getPath());
        }

        // 5) 如有变化则更新元数据
        if (changed) {
            statInfo.setReplicaData(validReplicas);
            metadataStorage.saveMetadata(fileSystemName, statInfo.getPath(), statInfo);
        }
        return changed;
    }

    /**
     * 过副本清理：保留主副本+前两个有效副本，其余删除并从元数据移除。
     */
    private boolean cleanupOverReplicas(StatInfo statInfo, List<ReplicaData> validReplicas) {
        if (validReplicas == null || validReplicas.size() <= 3) return false;
        List<ReplicaData> keep = new ArrayList<>(3);
        // 先保留主副本
        ReplicaData primary = validReplicas.stream().filter(r -> r.isPrimary).findFirst().orElse(null);
        if (primary != null) {
            keep.add(primary);
        }
        // 再按顺序补足到3个
        for (ReplicaData r : validReplicas) {
            if (keep.size() >= 3) break;
            if (primary != null && r == primary) continue;
            keep.add(r);
        }
        // 待移除列表
        Set<String> keepSet = new HashSet<>();
        for (ReplicaData r : keep) keepSet.add(r.dsNode);
        List<ReplicaData> toRemove = new ArrayList<>();
        for (ReplicaData r : validReplicas) {
            if (!keepSet.contains(r.dsNode)) {
                toRemove.add(r);
            }
        }
        // 物理删除多余副本
        int removedCount = 0;
        for (ReplicaData r : toRemove) {
            try {
                // 确定文件系统名
                String fsName = null;
                for (String n : metadataStorage.getAllFileSystemNames()) {
                    if (metadataStorage.exists(n, statInfo.getPath())) { fsName = n; break; }
                }
                if (fsName == null) {
                    log.warn("无法确定文件所属的文件系统，跳过删除副本: {}", statInfo.getPath());
                    continue;
                }
                boolean ok = dataServerClientService.deleteFileFromDataServer(fsName, r);
                if (ok) removedCount++;
                log.info("过副本清理，删除副本: {} -> {} 成功={} ", r.dsNode, statInfo.getPath(), ok);
            } catch (Exception e) {
                log.warn("过副本清理删除失败: {} -> {}", r.dsNode, statInfo.getPath(), e);
            }
        }
        // 更新元数据中的副本列表为keep
        if (!toRemove.isEmpty()) {
            validReplicas.clear();
            validReplicas.addAll(keep);
            return true;
        }
        return false;
    }

    /**
     * 历史脏数据清理：对于当前元数据未分配该文件副本的活跃DataServer，如存在该文件数据则删除。
     * 返回清理数量。
     */
    private int cleanupStaleReplicasNotInMetadata(String fileSystemName, StatInfo statInfo, List<ReplicaData> currentReplicas) {
        try {
            Set<String> assigned = new HashSet<>();
            if (currentReplicas != null) {
                for (ReplicaData r : currentReplicas) assigned.add(r.dsNode);
            }
            int cleaned = 0;
            List<Map<String, Object>> actives = zkDataServerService.getActiveDataServers();
            for (Map<String, Object> s : actives) {
                Object addrObj = s.get("address");
                if (addrObj == null) continue;
                String addr = String.valueOf(addrObj);
                if (assigned.contains(addr)) continue; // 该节点是被分配副本的，跳过
                boolean exists = false;
                try {
                    exists = dataServerClientService.checkFileExistsOnDataServer(addr, fileSystemName, statInfo.getPath());
                } catch (Exception e) {
                    // 忽略探测异常
                }
                if (exists) {
                    ReplicaData temp = new ReplicaData();
                    temp.dsNode = addr;
                    temp.path = statInfo.getPath();
                    boolean ok = dataServerClientService.deleteFileFromDataServer(fileSystemName, temp);
                    if (ok) cleaned++;
                    log.info("清理历史脏副本: {} -> {} 成功={} ", addr, statInfo.getPath(), ok);
                }
            }
            return cleaned;
        } catch (Exception e) {
            log.warn("清理历史脏副本时出错: {}", statInfo.getPath(), e);
            return 0;
        }
    }

    /**
     * 从活跃 DataServer 中选择目标节点，排除已使用的节点。
     */
    private List<String> chooseTargetDataServers(Set<String> exclude, int needed) {
        List<Map<String, Object>> actives = zkDataServerService.getActiveDataServers();
        List<String> candidates = new ArrayList<>();
        for (Map<String, Object> s : actives) {
            Object addr = s.get("address");
            if (addr == null) continue;
            String a = String.valueOf(addr);
            if (!exclude.contains(a)) {
                candidates.add(a);
            }
        }
        // 简单截断到需要数量（后续可替换为按负载/机架分布的打分选择）
        return candidates.size() > needed ? candidates.subList(0, needed) : candidates;
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

}

