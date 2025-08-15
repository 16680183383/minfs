package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.ReplicaData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class DataServerClientService {
    
    @Autowired
    private RestTemplate restTemplate;
    
    // 线程池用于并发调用DataServer接口
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    
    // 缓存DataServer连接状态
    private final Map<String, Boolean> dataServerStatus = new ConcurrentHashMap<>();
    
    /**
     * 向DataServer写入文件数据
     * @param replicaData 副本信息
     * @param data 文件数据
     * @param offset 偏移量
     * @param length 长度
     * @return 是否成功
     */
    public boolean writeToDataServer(ReplicaData replicaData, byte[] data, int offset, int length) {
        try {
            String url = "http://" + replicaData.dsNode + "/write";
            
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", "minfs");
            
            // 构建请求参数
            String path = replicaData.path;
            
            // 发送写入请求
            ResponseEntity<String> response = restTemplate.exchange(
                url + "?path=" + path + "&offset=" + offset + "&length=" + length,
                HttpMethod.POST,
                new HttpEntity<>(data, headers),
                String.class
            );
            
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("成功写入DataServer: {} -> {}", replicaData.dsNode, path);
                dataServerStatus.put(replicaData.dsNode, true);
                return true;
            } else {
                log.error("写入DataServer失败: {} -> {}, 状态码: {}", 
                         replicaData.dsNode, path, response.getStatusCode());
                return false;
            }
            
        } catch (Exception e) {
            log.error("写入DataServer异常: {} -> {}", replicaData.dsNode, replicaData.path, e);
            dataServerStatus.put(replicaData.dsNode, false);
            return false;
        }
    }
    
    /**
     * 从DataServer读取文件数据
     * @param replicaData 副本信息
     * @param offset 偏移量
     * @param length 长度
     * @return 文件数据
     */
    public byte[] readFromDataServer(ReplicaData replicaData, int offset, int length) {
        try {
            String url = "http://" + replicaData.dsNode + "/read";
            
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", "minfs");
            
            // 构建请求参数
            String path = replicaData.path;
            
            // 发送读取请求
            ResponseEntity<byte[]> response = restTemplate.exchange(
                url + "?path=" + path + "&offset=" + offset + "&length=" + length,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                byte[].class
            );
            
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("成功从DataServer读取: {} -> {}", replicaData.dsNode, path);
                dataServerStatus.put(replicaData.dsNode, true);
                return response.getBody();
            } else {
                log.error("从DataServer读取失败: {} -> {}, 状态码: {}", 
                         replicaData.dsNode, path, response.getStatusCode());
                return null;
            }
            
        } catch (Exception e) {
            log.error("从DataServer读取异常: {} -> {}", replicaData.dsNode, replicaData.path, e);
            dataServerStatus.put(replicaData.dsNode, false);
            return null;
        }
    }
    
    /**
     * 删除DataServer上的文件
     * @param replicaData 副本信息
     * @return 是否成功
     */
    public boolean deleteFromDataServer(ReplicaData replicaData) {
        try {
            String url = "http://" + replicaData.dsNode + "/delete";
            
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", "minfs");
            
            // 构建请求参数
            String path = replicaData.path;
            
            // 发送删除请求
            ResponseEntity<String> response = restTemplate.exchange(
                url + "?path=" + path,
                HttpMethod.DELETE,
                new HttpEntity<>(headers),
                String.class
            );
            
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("成功从DataServer删除: {} -> {}", replicaData.dsNode, path);
                return true;
            } else {
                log.error("从DataServer删除失败: {} -> {}, 状态码: {}", 
                         replicaData.dsNode, path, response.getStatusCode());
                return false;
            }
            
        } catch (Exception e) {
            log.error("从DataServer删除异常: {} -> {}", replicaData.dsNode, replicaData.path, e);
            return false;
        }
    }
    
    /**
     * 向多个DataServer并发写入数据（三副本同步）
     * @param replicas 副本列表
     * @param data 文件数据
     * @param offset 偏移量
     * @param length 长度
     * @return 成功写入的副本数量
     */
    public int writeToMultipleDataServers(List<ReplicaData> replicas, byte[] data, int offset, int length) {
        List<CompletableFuture<Boolean>> futures = replicas.stream()
            .map(replica -> CompletableFuture.supplyAsync(() -> 
                writeToDataServer(replica, data, offset, length), executorService))
            .toList();
        
        // 等待所有写入完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );
        
        try {
            allFutures.get(); // 等待完成
            
            // 统计成功数量
            long successCount = futures.stream()
                .mapToLong(future -> {
                    try {
                        return future.get() ? 1 : 0;
                    } catch (Exception e) {
                        log.error("获取写入结果异常", e);
                        return 0;
                    }
                })
                .sum();
            
            log.info("三副本写入完成: {}/{} 成功", successCount, replicas.size());
            return (int) successCount;
            
        } catch (Exception e) {
            log.error("等待写入完成异常", e);
            return 0;
        }
    }
    
    /**
     * 从多个DataServer读取数据（故障转移）
     * @param replicas 副本列表
     * @param offset 偏移量
     * @param length 长度
     * @return 文件数据
     */
    public byte[] readFromMultipleDataServers(List<ReplicaData> replicas, int offset, int length) {
        // 优先尝试主副本
        for (ReplicaData replica : replicas) {
            if (replica.isPrimary) {
                byte[] data = readFromDataServer(replica, offset, length);
                if (data != null) {
                    return data;
                }
            }
        }
        
        // 主副本失败，尝试其他副本
        for (ReplicaData replica : replicas) {
            if (!replica.isPrimary) {
                byte[] data = readFromDataServer(replica, offset, length);
                if (data != null) {
                    return data;
                }
            }
        }
        
        log.error("所有副本读取都失败");
        return null;
    }
    
    /**
     * 检查DataServer状态
     * @param dsNode DataServer节点地址
     * @return 是否可用
     */
    public boolean isDataServerAvailable(String dsNode) {
        return dataServerStatus.getOrDefault(dsNode, true);
    }
    
    /**
     * 获取DataServer状态统计
     * @return 状态统计信息
     */
    public Map<String, Object> getDataServerStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("totalDataServers", dataServerStatus.size());
        status.put("availableDataServers", 
            dataServerStatus.values().stream().mapToLong(available -> available ? 1 : 0).sum());
        status.put("unavailableDataServers", 
            dataServerStatus.values().stream().mapToLong(available -> available ? 0 : 1).sum());
        status.put("statusDetails", new ConcurrentHashMap<>(dataServerStatus));
        return status;
    }
    
    /**
     * 清理资源
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}
