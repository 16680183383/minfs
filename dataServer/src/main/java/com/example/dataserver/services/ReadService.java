/*
* 优先读取本地，读取失败后切换其他副本节点
* */

package com.example.dataserver.services;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
//import java.io.Files;
import java.nio.file.Files;
import java.util.*;
        import java.util.stream.Collectors;

@Service
public class ReadService {

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private RegistService registService; // 用于获取副本节点信息

    @Value("${dataserver.ip}")
    private String selfIp;
    @Value("${server.port}")
    private int selfPort;
    @Value("${dataserver.storage.path}")
    private String localStoragePath;

    private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64MB/块
    private static final int READ_RETRY_TIMES = 2; // 每个节点的读取重试次数


    /**
     * 扩展：支持从本地或副本节点分块读取文件，并校验MD5
     * 逻辑：先尝试本地读取，失败则依次从其他副本节点读取
     */
    public byte[] readWithChunk(String path, List<String> expectedChunkMd5) {
        try {
            // 1. 获取文件的所有副本节点（包括本地节点，共3个）
            List<String> replicaNodes = getReplicaNodes(path);
            if (replicaNodes.isEmpty()) {
                throw new RuntimeException("No replica nodes found for file: " + path);
            }

            // 2. 遍历目录获取所有块（本地目录结构）
            File chunkDir = new File(getLocalFilePath(path));
            List<File> sortedChunks = getSortedChunkFiles(chunkDir, path);

            // 3. 分块读取（优先本地，失败则从副本读取）
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < sortedChunks.size(); i++) {
                String chunkName = sortedChunks.get(i).getName(); // 如"chunk_0"
                byte[] chunkData = null;

                // 3.1 先尝试从本地读取
                try {
                    chunkData = readLocalChunk(path, chunkName);
                } catch (Exception e) {
                    System.err.println("Local read failed for chunk " + chunkName + ", trying replicas");
                }

                // 3.2 本地读取失败，从副本节点读取
                if (chunkData == null) {
                    chunkData = readChunkFromReplicas(path, chunkName, replicaNodes);
                }

                // 3.3 校验块MD5
                String actualMd5 = DigestUtils.md5Hex(chunkData);
                if (!actualMd5.equals(expectedChunkMd5.get(i))) {
                    throw new RuntimeException("Chunk " + i + " MD5 mismatch");
                }

                bos.write(chunkData);
            }

            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Read with chunk failed: " + e.getMessage(), e);
        }
    }


    /**
     * 从副本节点读取指定块
     * 逻辑：遍历所有副本节点（排除本地），逐个尝试读取
     */
    private byte[] readChunkFromReplicas(String path, String chunkName, List<String> replicaNodes) {
        String selfNode = selfIp + ":" + selfPort;
        // 过滤掉本地节点，优先尝试其他副本
        List<String> remoteReplicas = replicaNodes.stream()
                .filter(node -> !node.equals(selfNode))
                .collect(Collectors.toList());

        for (String node : remoteReplicas) {
            if (!isNodeAvailable(node)) {
                System.err.println("Replica node " + node + " is unavailable, skip");
                continue;
            }

            // 尝试从该副本节点读取块
            for (int retry = 0; retry < READ_RETRY_TIMES; retry++) {
                try {
                    return readRemoteChunk(node, path, chunkName);
                } catch (Exception e) {
                    System.err.println("Retry " + (retry + 1) + " failed for node " + node);
                    if (retry == READ_RETRY_TIMES - 1) {
                        break; // 达到最大重试次数，切换节点
                    }
                }
            }
        }

        throw new RuntimeException("All replica nodes failed to read chunk: " + chunkName);
    }


    /**
     * 从远程节点读取块（调用目标节点的read接口）
     */
    private byte[] readRemoteChunk(String node, String path, String chunkName) {
        String[] nodeInfo = node.split(":");
        String nodeIp = nodeInfo[0];
        int nodePort = Integer.parseInt(nodeInfo[1]);
        String chunkPath = path + "/" + chunkName; // 块的完整路径（如/test/file/chunk_0）

        // 调用远程节点的read接口（需与目标节点的接口定义一致）
        String url = String.format(
                "http://%s:%d/read?path=%s&offset=0&length=%d",
                nodeIp, nodePort, chunkPath, BLOCK_SIZE
        );

        ResponseEntity<byte[]> response = restTemplate.getForEntity(url, byte[].class);
        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            return response.getBody();
        } else {
            throw new RuntimeException("Remote node " + node + " read failed");
        }
    }


    // ---------------------- 辅助方法 ----------------------

    /**
     * 获取文件的所有副本节点（从元数据服务/注册中心获取）
     */
    private List<String> getReplicaNodes(String path) {
        // 实际应从metaServer查询文件元数据中的副本列表
        // 此处简化：从注册的dataServer中获取3个副本（包含本地）
        List<Map<String, Object>> allServers = registService.getDslist();
        return allServers.stream()
                .map(ds -> ds.get("ip") + ":" + ds.get("port"))
                .limit(3) // 取前3个作为副本（实际应按元数据记录）
                .collect(Collectors.toList());
    }

    /**
     * 检查节点是否可用（通过健康检查接口）
     */
    private boolean isNodeAvailable(String node) {
        try {
            String[] nodeInfo = node.split(":");
            String healthUrl = "http://" + nodeInfo[0] + ":" + nodeInfo[1] + "/health";
            ResponseEntity<String> response = restTemplate.getForEntity(healthUrl, String.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 读取本地块文件
     */
    private byte[] readLocalChunk(String path, String chunkName) throws Exception {
        String localChunkPath = getLocalFilePath(path) + File.separator + chunkName;
        File chunkFile = new File(localChunkPath);
        if (!chunkFile.exists() || !chunkFile.isFile()) {
            throw new RuntimeException("Local chunk not found: " + chunkName);
        }
        return Files.readAllBytes(chunkFile.toPath());
    }

    /**
     * 获取排序后的块文件列表（按chunk_0、chunk_1...顺序）
     */
    private List<File> getSortedChunkFiles(File chunkDir, String path) {
        if (!chunkDir.exists()) {
            throw new RuntimeException("File not found: " + path);
        }

        File[] chunkFiles = chunkDir.listFiles((dir, name) -> name.startsWith("chunk_"));
        if (chunkFiles == null || chunkFiles.length == 0) {
            throw new RuntimeException("No chunks found for: " + path);
        }

        // 按块号排序（chunk_0 < chunk_1 < ...）
        return Arrays.stream(chunkFiles)
                .sorted(Comparator.comparingInt(f -> {
                    String[] nameParts = f.getName().split("_");
                    return Integer.parseInt(nameParts[1]);
                }))
                .collect(Collectors.toList());
    }

    /**
     * 生成本地文件路径
     */
    private String getLocalFilePath(String path) {
        return localStoragePath + File.separator + path.replace("/", File.separator);
    }
}
