package com.example.dataserver.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class FileDeleteService {

    @Autowired
    private RegistService registService;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DataService dataService;  // 复用DataService中的工具方法

    @Value("${dataserver.ip}")
    private String selfIp;

    @Value("${server.port}")
    private int selfPort;

    private final int RETRY_TIMES = 3; // 与DataService保持一致的重试次数
    private final long RETRY_INTERVAL = 1000; // 重试间隔（ms）

    /**
     * 删除文件（支持原始删除和副本同步删除）
     * @param path 文件路径（如/test/large_file.bin）
     * @param isReplicaSync 是否为副本同步请求（true=仅删除本地，false=删除本地+所有副本）
     * @return 本地删除是否成功（原始请求返回“所有副本是否删除成功”，同步请求返回“本地是否删除成功”）
     */
    public boolean deleteFile(String path, boolean isReplicaSync) {
        try {
            // 1. 无论何种请求，先删除本地文件（分块+MD5清单）
            boolean localDeleteSuccess = deleteLocalFile(path);
            if (!localDeleteSuccess) {
                throw new RuntimeException("本地文件删除失败：" + path);
            }

            // 2. 分支判断：仅原始请求（非副本同步）需要删除远程副本
            if (!isReplicaSync) {
                // 原始请求：获取所有副本位置，删除远程节点文件
                List<String> replicaLocations = getReplicaLocations(path);
                if (replicaLocations == null || replicaLocations.isEmpty()) {
                    System.out.println("[WARN] 未找到文件" + path + "的副本信息，仅完成本地删除");
                    return true; // 本地已删除，视为“部分成功”
                }

                String selfLocation = selfIp + ":" + selfPort;
                boolean allRemoteSuccess = true;

                // 遍历删除所有远程副本（排除本地节点）
                for (String location : replicaLocations) {
                    if (location.equals(selfLocation)) {
                        continue;
                    }

                    String[] parts = location.split(":");
                    if (parts.length != 2) {
                        System.err.println("[ERROR] 无效的副本位置格式：" + location);
                        allRemoteSuccess = false;
                        continue;
                    }
                    String dsIp = parts[0];
                    int dsPort = Integer.parseInt(parts[1]);

                    // 调用远程删除接口（带重试）
                    boolean remoteSuccess = deleteFromReplica(dsIp, dsPort, path);
                    if (!remoteSuccess) {
                        System.err.println("[ERROR] 远程副本" + location + "删除失败");
                        allRemoteSuccess = false;
                    }
                }

                System.out.println("[INFO] 原始删除请求完成，本地删除成功，远程副本" + (allRemoteSuccess ? "全部" : "部分") + "删除成功");
                return allRemoteSuccess; // 原始请求返回“所有副本是否删除成功”

            } else {
                // 副本同步请求：仅删除本地，不触发远程删除（避免循环）
                System.out.println("[INFO] 副本同步删除请求，仅完成本地文件删除：" + path);
                return true; // 同步请求只需返回“本地是否删除成功”
            }

        } catch (Exception e) {
            System.err.println("[ERROR] 删除文件失败：" + e.getMessage());
            throw new RuntimeException("Delete file failed", e);
        }
    }

    /**
     * 删除本地文件（包括分块目录和MD5清单）
     * @param path 文件路径
     * @return 是否删除成功
     */
    private boolean deleteLocalFile(String path) {
        String localFilePath = dataService.getLocalFilePath(path); // 复用DataService的路径生成逻辑
        File file = new File(localFilePath);
        System.out.println("[INFO] 开始删除本地文件：" + localFilePath);

        if (!file.exists()) {
            System.out.println("[WARN] 本地文件不存在，跳过删除：" + localFilePath);
            return true;
        }

        // 递归删除目录（文件分块存储在目录中）
        if (deleteDirectory(file)) {
            System.out.println("[INFO] 本地文件删除成功：" + localFilePath);
            return true;
        } else {
            System.err.println("[ERROR] 本地文件删除失败：" + localFilePath);
            return false;
        }
    }

    /**
     * 递归删除目录及其中所有文件
     */
    private boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    boolean success = deleteDirectory(child);
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        // 目录为空或本身是文件，直接删除
        return dir.delete();
    }

    /**
     * 从副本节点删除文件（带重试机制）
     * @param dsIp 节点IP
     * @param dsPort 节点端口
     * @param path 文件路径
     * @return 是否删除成功
     */
    private boolean deleteFromReplica(String dsIp, int dsPort, String path) {
        try {
            String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.name());
            String url = "http://" + dsIp + ":" + dsPort + "/delete?path=" + encodedPath;

            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", "minfs");
            // 关键：传递副本同步标识，告知目标节点“仅删除本地”
            headers.set("X-Is-Replica-Sync", "true");
            HttpEntity<Void> request = new HttpEntity<>(headers);

            System.out.println("[INFO] 开始删除远程副本：" + dsIp + ":" + dsPort + "，路径：" + path);

            for (int i = 0; i < RETRY_TIMES; i++) {
                try {
                    System.out.println("[DEBUG] 第" + (i+1) + "次删除重试，URL：" + url);
                    ResponseEntity<Void> response = restTemplate.exchange(
                            url,
                            HttpMethod.DELETE,
                            request,
                            Void.class
                    );

                    if (response.getStatusCode().is2xxSuccessful()) {
                        System.out.println("[INFO] 远程副本" + dsIp + ":" + dsPort + "删除成功");
                        return true;
                    } else {
                        System.err.println("[WARN] 第" + (i+1) + "次删除响应非成功状态：" + response.getStatusCode());
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] 第" + (i+1) + "次删除远程副本失败：" + e.getMessage());
                    try {
                        Thread.sleep(RETRY_INTERVAL);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            return false;
        } catch (UnsupportedEncodingException e) {
            System.err.println("[ERROR] 路径编码失败：" + e.getMessage());
            return false;
        }
    }

    /**
     * 从元数据服务获取文件的所有副本位置
     * @param path 文件路径
     * @return 副本位置列表（ip:port）
     */
    private List<String> getReplicaLocations(String path) {
        // 复用注册服务查询副本位置（与DataService的副本管理逻辑保持一致）
        return dataService.getCachedReplicaLocations(path);
    }
}