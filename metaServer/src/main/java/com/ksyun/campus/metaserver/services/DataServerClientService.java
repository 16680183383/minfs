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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
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
    
    @Autowired
    private ZkDataServerService zkDataServerService;
    
    // 线程池用于并发调用DataServer接口
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    
    // 缓存DataServer连接状态
    private final Map<String, Boolean> dataServerStatus = new ConcurrentHashMap<>();

    /**
     * 删除文件时通知DataServer删除实际数据
     * @param fileSystemName 文件系统名称
     * @param replicaData 副本信息
     * @return 是否成功
     */
    public boolean deleteFileFromDataServer(String fileSystemName, ReplicaData replicaData) {
        try {
            String url = "http://" + replicaData.dsNode + "/delete";
            
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", fileSystemName);
            
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
                log.info("成功从DataServer删除: fileSystemName={}, {} -> {}", fileSystemName, replicaData.dsNode, path);
                return true;
            } else {
                log.error("从DataServer删除失败: fileSystemName={}, {} -> {}, 状态码: {}", 
                         fileSystemName, replicaData.dsNode, path, response.getStatusCode());
                return false;
            }
            
        } catch (Exception e) {
            log.error("从DataServer删除异常: fileSystemName={}, {} -> {}", fileSystemName, replicaData.dsNode, replicaData.path, e);
            return false;
        }
    }

    /**
     * 直接向指定 DataServer 写入数据，携带副本同步标记，避免被对端继续级联复制。
     */
    public boolean writeDirectToDataServer(String dsAddress, String fileSystemName, String path, int offset, int length, byte[] data) {
        try {
            String url = "http://" + dsAddress + "/write";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM);
            headers.set("X-Is-Replica-Sync", "true");
            headers.set("fileSystemName", fileSystemName);

            // 修复：避免重复编码，直接使用原始路径
            String queryParams = String.format("?path=%s&offset=%d&length=%d", path, offset, length);
            ResponseEntity<String> response = restTemplate.exchange(
                    url + queryParams,
                    HttpMethod.POST,
                    new HttpEntity<>(data, headers),
                    String.class
            );
            boolean ok = response.getStatusCode().is2xxSuccessful();
            if (ok) {
                dataServerStatus.put(dsAddress, true);
            }
            return ok;
        } catch (Exception e) {
            log.error("直接写入DataServer失败: fileSystemName={}, {} -> {}", fileSystemName, dsAddress, path, e);
            return false;
        }
    }

    /**
     * 调用DataServer检查文件是否存在（检查第一个分块）
     */
    public boolean checkFileExistsOnDataServer(String dsAddress, String fileSystemName, String path) {
        try {
            // 修复：避免重复编码，直接使用原始路径
            String url = "http://" + dsAddress + "/checkFileExists?path=" + path;
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", fileSystemName);
            
            ResponseEntity<Boolean> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    Boolean.class
            );
            return response.getStatusCode().is2xxSuccessful() && Boolean.TRUE.equals(response.getBody());
        } catch (Exception e) {
            log.warn("检查副本是否存在失败: fileSystemName={}, {} -> {}", fileSystemName, dsAddress, path, e);
            return false;
        }
    }

    /**
     * 从指定DataServer读取文件的全部数据（简化：服务端忽略offset/length，返回完整内容）
     */
    public byte[] readFromDataServer(String dsAddress, String fileSystemName, String path) {
        try {
            // 修复：避免重复编码，直接使用原始路径
            String url = "http://" + dsAddress + "/read?path=" + path + "&offset=0&length=-1";
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", fileSystemName);
            
            ResponseEntity<byte[]> resp = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    byte[].class
            );
            if (resp.getStatusCode().is2xxSuccessful()) {
                return resp.getBody();
            }
            return null;
        } catch (Exception e) {
            log.error("从DataServer读取文件失败: fileSystemName={}, {} -> {}", fileSystemName, dsAddress, path, e);
            return null;
        }
    }

    /**
     * 将数据作为副本写入到指定DataServer（带副本同步标记，避免二次级联）
     */
    public boolean writeReplicaToDataServer(String dsAddress, String fileSystemName, String path, byte[] data) {
        try {
            // 修复：避免重复编码，直接使用原始路径
            String url = "http://" + dsAddress + "/write?path=" + path + "&offset=0&length=" + (data == null ? 0 : data.length);
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Is-Replica-Sync", "true");
            headers.set("fileSystemName", fileSystemName);
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM);
            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    new HttpEntity<>(data, headers),
                    String.class
            );
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.error("写入副本到DataServer失败: fileSystemName={}, {} -> {}", fileSystemName, dsAddress, path, e);
            return false;
        }
    }

    /**
     * 在两个DataServer之间复制文件（源读取，目标写入为副本）
     */
    public boolean replicateBetweenDataServers(String sourceAddress, String targetAddress, String fileSystemName, String path) {
        byte[] data = readFromDataServer(sourceAddress, fileSystemName, path);
        if (data == null || data.length == 0) {
            log.warn("源DataServer返回空数据，放弃复制: fileSystemName={}, {} -> {} ({})", fileSystemName, sourceAddress, targetAddress, path);
            return false;
        }
        return writeReplicaToDataServer(targetAddress, fileSystemName, path, data);
    }

    
    /**
     * 从多个DataServer并发删除文件
     * @param fileSystemName 文件系统名称
     * @param replicas 副本列表
     * @return 成功删除的副本数量
     */
    public int deleteFromMultipleDataServers(String fileSystemName, List<ReplicaData> replicas) {
        List<CompletableFuture<Boolean>> futures = replicas.stream()
            .map(replica -> CompletableFuture.supplyAsync(() -> 
                deleteFileFromDataServer(fileSystemName, replica), executorService))
            .toList();
        
        // 等待所有删除完成
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
                        log.error("获取删除结果异常", e);
                        return 0;
                    }
                })
                .sum();
            
            log.info("多副本删除完成: fileSystemName={}, {}/{} 成功", fileSystemName, successCount, replicas.size());
            return (int) successCount;
            
        } catch (Exception e) {
            log.error("等待删除完成异常: fileSystemName={}", fileSystemName, e);
            return 0;
        }
    }
    
    /**
     * 获取DataServer状态信息
     * @return DataServer状态
     */
    public Map<String, Object> getDataServerStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("dataServerStatus", new HashMap<>(dataServerStatus));
        status.put("totalDataServers", dataServerStatus.size());
        status.put("activeDataServers", dataServerStatus.values().stream()
            .mapToInt(available -> available ? 1 : 0).sum());
        return status;
    }
}
