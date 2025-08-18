package com.example.dataserver.services;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class DataService {

    @Autowired
    private RegistService registService;
    @Autowired
    private RestTemplate restTemplate;

    @Value("${dataserver.storage.path}")
    private String localStoragePath; // 本地存储根目录（如/data/dataserver/storage）
    @Value("${dataserver.ip}")
    private String selfIp;
    @Value("${server.port}")
    private int selfPort;

    private final int REPLICA_COUNT = 3; // 三副本，符合文档要求
    private final int RETRY_TIMES = 3; // 重试次数
    private final long RETRY_INTERVAL = 1000; // 重试间隔（ms）
    // 新增块大小常量（64MB）
    private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64MB
    // 新增轮询计数器（记录上次选择的索引，简化实现）
    private static int roundRobinIndex = 0;

    // 缓存：文件路径 -> 副本位置列表（仅适合小型系统，重启后失效）
    private final Map<String, List<String>> fileReplicaCache = new ConcurrentHashMap<>();

    /**
     * 计算节点的剩余容量（总容量-已用容量）
     */
    private long getRemainingCapacity(Map<String, Object> ds) {
        Number totalNum = (Number) ds.get("totalCapacity"); // 先转为Number
        Number usedNum = (Number) ds.get("usedCapacity");
        return totalNum.longValue() - usedNum.longValue(); // 统一转为long，强制转换改为安全转换
    }

    public void write(byte[] data){
        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，以达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
    }

    /**
     * 写入数据到本地并同步副本，符合"三副本写均衡调度"要求
     * @param data 待写入的二进制数据
     * @param path 文件路径（用于生成本地存储路径）
     * @return 三副本位置（格式：ip:port）
     */
    public List<String> write(byte[] data, String path) {
        try {
            // 1. 写入本地文件系统
            String localFilePath = getLocalFilePath(path);
            writeToLocal(localFilePath, data, 0); // 从偏移量0写入

            // 2. 选择符合策略的副本节点（按az->rack分布，避免同az）
            List<Map<String, Object>> candidateDs = selectReplicaNodes(path);

            // 3. 同步数据到副本节点（当前节点+2个候选节点，共3个）
            List<String> replicaLocations = new ArrayList<>();
            replicaLocations.add(selfIp + ":" + selfPort); // 本节点作为主副本

            for (Map<String, Object> ds : candidateDs) {
                String dsIp = (String) ds.get("ip");
                int dsPort = (int) ds.get("port");
                boolean syncSuccess = syncToReplica(dsIp, dsPort, data, path);
                if (syncSuccess) {
                    replicaLocations.add(dsIp + ":" + dsPort);
                } else {
                    throw new RuntimeException("Failed to sync replica to " + dsIp + ":" + dsPort);
                }
            }

            return replicaLocations;
        } catch (Exception e) {
            throw new RuntimeException("Write data failed", e);
        }
    }

    /**
     * 读取文件指定范围的数据（支持分块存储的文件）
     * @param path 文件完整路径（如/test/2.txt）
     * @param offset 读取起始偏移量（从0开始）
     * @param length 读取字节数
     * @return 读取的二进制数据
     */
    public byte[] read(String path, int offset, int length) {
        try {
            System.out.println("[INFO] 开始读取文件：" + path + "，偏移量：" + offset + "，长度：" + length + "字节");

            // 1. 验证参数合法性
            if (offset < 0 || length <= 0) {
                throw new IllegalArgumentException("无效的偏移量或长度：offset=" + offset + ", length=" + length);
            }

            // 2. 确定块大小（与写入时保持一致，如64MB）
            int blockSize = BLOCK_SIZE; // 需与writeWithChunk中的块大小一致
            System.out.println("[INFO] 块大小：" + blockSize + "字节");

            // 3. 计算涉及的块范围
            int startBlockIndex = offset / blockSize; // 起始块索引
            int endBlockIndex = (offset + length - 1) / blockSize; // 结束块索引（向上取整）
            System.out.println("[INFO] 涉及块范围：" + startBlockIndex + "~" + endBlockIndex);

            // 4. 读取每个块的对应部分并拼接
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (int i = startBlockIndex; i <= endBlockIndex; i++) {
                // 构造块路径（如/test/2.txt/chunk_0）
                String chunkPath = path + "/chunk_" + i;
                String localChunkPath = getLocalFilePath(chunkPath);
                File chunkFile = new File(localChunkPath);

                // 检查块文件是否存在
                if (!chunkFile.exists()) {
                    throw new RuntimeException("块文件不存在：" + localChunkPath);
                }

                // 计算在当前块中的读取偏移量和长度
                int chunkOffset = (i == startBlockIndex) ? (offset % blockSize) : 0;
                int remaining = length - outputStream.size(); // 剩余需读取的总长度
                int chunkReadLength = Math.min(remaining, blockSize - chunkOffset);

                System.out.println("[INFO] 读取块 " + i + "，块内偏移：" + chunkOffset + "，读取长度：" + chunkReadLength);

                // 读取块文件的指定部分
                try (RandomAccessFile raf = new RandomAccessFile(chunkFile, "r")) {
                    raf.seek(chunkOffset);
                    byte[] buffer = new byte[chunkReadLength];
                    int bytesRead = raf.read(buffer);
                    if (bytesRead != chunkReadLength) {
                        throw new RuntimeException("块 " + i + " 读取不完整，预期：" + chunkReadLength + "，实际：" + bytesRead);
                    }
                    outputStream.write(buffer);
                }

                // 若已读取足够长度，提前退出循环
                if (outputStream.size() >= length) {
                    break;
                }
            }

            // 5. 验证总读取长度是否符合预期
            byte[] result = outputStream.toByteArray();
            if (result.length != length) {
                throw new RuntimeException("读取长度不符，预期：" + length + "，实际：" + result.length);
            }

            System.out.println("[INFO] 文件读取完成，总长度：" + result.length + "字节");
            return result;
        } catch (Exception e) {
            System.err.println("[ERROR] 读取文件失败：" + e.getMessage());
            throw new RuntimeException("Read file failed", e);
        }
    }

    /**
     * 写入数据到本地磁盘
     */
    private void writeToLocal(String localFilePath, byte[] data, int offset) throws Exception {
        File file = new File(localFilePath);
        System.out.println("[INFO] 准备写入本地文件：" + localFilePath + "，数据大小：" + data.length + "字节"); // 新增日志
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
            System.out.println("[INFO] 创建父目录：" + file.getParentFile().getAbsolutePath()); // 新增日志
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            raf.write(data);
            System.out.println("[INFO] 本地写入完成：" + localFilePath); // 新增日志
        } catch (Exception e) {
            System.err.println("[ERROR] 本地写入失败：" + e.getMessage()); // 新增错误日志
            throw e;
        }
    }

    /**
     * 同步数据到副本节点（调用目标dataServer的write接口）
     */
    private boolean syncToReplica(String dsIp, int dsPort, byte[] data, String path) throws UnsupportedEncodingException {
        // 新增：先检查目标节点文件是否已存在（通过调用目标节点的"文件存在检查"接口）
        if (checkFileExistsOnReplica(dsIp, dsPort, path)) {
            System.out.println("[INFO] 目标节点" + dsIp + ":" + dsPort + "已存在文件，跳过同步");
            return true;
        }
        // String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.name());
        String url = "http://" + dsIp + ":" + dsPort + "/write?path=" + path + "&offset=0&length=" + data.length;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        // 添加必需的fileSystemName请求头（与本节点一致，如"minfs"）
        headers.set("fileSystemName", "minfs");
        // 添加副本同步标识，用于目标节点判断请求类型
        headers.set("X-Is-Replica-Sync", "true");
        HttpEntity<byte[]> request = new HttpEntity<>(data, headers);


        // 打印同步请求基本信息
        System.out.println("[INFO] 开始同步至副本节点：" + dsIp + ":" + dsPort + "，目标路径：" + path + "，编码后路径：" + path);
        System.out.println("[INFO] 同步数据长度：" + data.length + "字节，与参数length是否一致：" + (data.length == Integer.parseInt(url.split("length=")[1])));

        for (int i = 0; i < RETRY_TIMES; i++) {
            try {
                System.out.println("[DEBUG] 第" + (i+1) + "次重试，请求URL：" + url);
                System.out.println("[DEBUG] 请求头信息：" + headers);

                ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
                System.out.println("[DEBUG] 副本节点响应状态码：" + response.getStatusCode());
                System.out.println("[DEBUG] 副本节点响应体：" + response.getBody());

                if (response.getStatusCode().is2xxSuccessful()) {
                    System.out.println("[INFO] 同步至" + dsIp + ":" + dsPort + "成功");
                    return true;
                } else {
                    System.err.println("[WARN] 第" + (i+1) + "次重试响应非成功状态：" + response.getStatusCode());
                }
            } catch (Exception e) {
                System.err.println("[ERROR] 第" + (i+1) + "次重试同步至" + dsIp + ":" + dsPort + "失败，异常信息：" + e.getMessage());
                e.printStackTrace(); // 打印堆栈跟踪，定位异常类型（如连接超时、接口不存在等）
                try {
                    Thread.sleep(RETRY_INTERVAL);
                    System.out.println("[INFO] 等待" + RETRY_INTERVAL + "ms后进行下一次重试");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.err.println("[ERROR] 重试等待被中断：" + ie.getMessage());
                    break; // 中断后退出重试循环
                }
            }
        }
        System.err.println("[ERROR] 所有" + RETRY_TIMES + "次重试均失败，同步至" + dsIp + ":" + dsPort + "最终失败");
        return false;
    }

    /**
     * 选择副本节点（轮询 + 剩余容量权重）
     */
    private List<Map<String, Object>> selectReplicaNodes(String path) {
        List<Map<String, Object>> allDs = registService.getDslist();
        // 1. 基础过滤：排除本节点+剩余容量>0
        List<Map<String, Object>> candidateDs = allDs.stream()
                .filter(ds -> !(ds.get("ip").equals(selfIp) && (int) ds.get("port") == selfPort))
                .filter(ds -> getRemainingCapacity(ds) > 0)
                .collect(Collectors.toList());

        if (candidateDs.size() < 2) {
            throw new RuntimeException("Not enough available dataServer (need 2, got " + candidateDs.size() + ")");
        }

        // 2. 优先筛选不同AZ的节点（满足跨AZ分布）
        String selfAz = getSelfAz();
        Map<String, List<Map<String, Object>>> azToDsMap = candidateDs.stream()
                .collect(Collectors.groupingBy(ds -> (String) ds.get("zone")));
        List<Map<String, Object>> crossAzCandidates = new ArrayList<>();
        // 先添加其他AZ的节点
        for (Map.Entry<String, List<Map<String, Object>>> entry : azToDsMap.entrySet()) {
            if (!entry.getKey().equals(selfAz)) {
                crossAzCandidates.addAll(entry.getValue());
            }
        }
        // 若跨AZ节点不足，补充本AZ节点（极端情况）
        if (crossAzCandidates.size() < 2) {
            crossAzCandidates.addAll(azToDsMap.getOrDefault(selfAz, new ArrayList<>()));
        }
        // 确保候选集至少有2个节点
        if (crossAzCandidates.size() < 2) {
            throw new RuntimeException("Not enough cross-AZ dataServer for replicas");
        }

        // 3. 对跨AZ候选集应用“剩余容量权重+轮询”（满足均衡调度）
        long totalRemaining = crossAzCandidates.stream()
                .mapToLong(this::getRemainingCapacity)
                .sum();
        List<Map<String, Object>> weightedCandidates = crossAzCandidates.stream()
                .peek(ds -> {
                    long remaining = getRemainingCapacity(ds);
                    ds.put("weight", (double) remaining / totalRemaining);
                })
                .sorted((d1, d2) -> Double.compare((double) d2.get("weight"), (double) d1.get("weight")))
                .collect(Collectors.toList());

        // 从权重前3的节点中轮询选择2个
        List<Map<String, Object>> topCandidates = weightedCandidates.size() > 3 ? weightedCandidates.subList(0, 3) : weightedCandidates;
        List<Map<String, Object>> selected = new ArrayList<>();
        int size = topCandidates.size();
        for (int i = 0; i < 2; i++) {
            int index = (roundRobinIndex + i) % size;
            selected.add(topCandidates.get(index));
        }
        roundRobinIndex = (roundRobinIndex + 2) % size;

        return selected;
    }

    /**
     * 生成本地存储路径（基于文件路径映射）
     */
    public String getLocalFilePath(String path) {
        // 例如：path为"/test/file.txt"，映射为localStoragePath/test/file.txt
        return localStoragePath + path.replace("/", File.separator);
    }

    /**
     * 获取本节点的az（从注册信息中获取）
     */
    private String getSelfAz() {
        // 实际应从zk的本节点数据中读取，此处简化
        return registService.getDslist().stream()
                .filter(ds -> ds.get("ip").equals(selfIp) && (int) ds.get("port") == selfPort)
                .map(ds -> (String) ds.get("zone"))
                .findFirst()
                .orElse("default-az");
    }

    /**
     * 分块写入文件并返回三副本位置（符合controller返回值要求）
     * @param data 待写入的二进制数据
     * @param path 文件路径
     * @return 三副本位置列表（格式：ip:port）
     */
    public List<String> writeWithChunk(byte[] data, String path, boolean isReplicaSync) {
        try {
            List<String> replicaLocations = new ArrayList<>();
            replicaLocations.add(selfIp + ":" + selfPort); // 本节点始终作为副本之一
            System.out.println("[INFO] 分块写入开始，目标路径：" + path + "，本节点：" + selfIp + ":" + selfPort + "，是否为副本同步请求：" + isReplicaSync);

            // 新增：存储每个块的MD5（用于后续校验）
            List<String> chunkMd5List = new ArrayList<>();

            // 分块处理（无论请求类型，都需写入本地）
            int totalChunks = (int) Math.ceil((double) data.length / BLOCK_SIZE);
            System.out.println("[INFO] 数据分块：" + totalChunks + "块，块大小：" + BLOCK_SIZE + "字节");

            for (int i = 0; i < totalChunks; i++) {
                int offset = i * BLOCK_SIZE;
                int length = Math.min(BLOCK_SIZE, data.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(data, offset, chunk, 0, length);
                System.out.println("[INFO] 处理块 " + i + "，大小：" + length + "字节");

                // 新增：计算当前块的MD5并记录
                String chunkMd5 = DigestUtils.md5Hex(chunk);
                chunkMd5List.add(chunkMd5);
                System.out.println("[INFO] 块 " + i + " MD5：" + chunkMd5);

                // 写入本地块（所有请求都执行）
                String chunkPath = path + "/chunk_" + i;
                writeToLocal(getLocalFilePath(chunkPath), chunk, 0);
            }

            // 2. 生成MD5清单文件（与分块文件同目录，不依赖metaServer）
            String md5ListPath = path + "/md5_list.txt";
            String localMd5ListPath = getLocalFilePath(md5ListPath);
            // 新增：将List<String>格式化为字符串，再转为byte[]（适配方法参数）
            StringBuilder md5Content = new StringBuilder();
            for (int i = 0; i < chunkMd5List.size(); i++) {
                // 格式：块索引,MD5（每行一条，便于后续读取解析）
                md5Content.append(i).append(",").append(chunkMd5List.get(i)).append(System.lineSeparator());
            }
            byte[] md5ListBytes = md5Content.toString().getBytes(StandardCharsets.UTF_8); // 转为byte[]
            writeMd5ListToLocal(localMd5ListPath, md5ListBytes);
            System.out.println("[INFO] MD5清单文件已生成：" + localMd5ListPath);

            // 核心修改：仅原始请求（非副本同步）需要同步至其他节点
            if (!isReplicaSync) {

                // 选择2个副本节点（满足三副本要求）
                List<Map<String, Object>> candidateDs = selectReplicaNodes(path);
                System.out.println("[INFO] 选中副本节点：" + candidateDs.stream()
                        .map(ds -> ds.get("ip") + ":" + ds.get("port"))
                        .collect(Collectors.joining(",")));

                // 同步每个块至副本节点
                for (int i = 0; i < totalChunks; i++) {
                    int offset = i * BLOCK_SIZE;
                    int length = Math.min(BLOCK_SIZE, data.length - offset);
                    byte[] chunk = new byte[length];
                    System.arraycopy(data, offset, chunk, 0, length);
                    String chunkPath = path + "/chunk_" + i;
                    String chunkMd5 = chunkMd5List.get(i); // 当前块的MD5

                    for (Map<String, Object> ds : candidateDs) {
                        String dsIp = (String) ds.get("ip");
                        int dsPort = (int) ds.get("port");
                        if (!syncToReplica(dsIp, dsPort, chunk, path)) {
                            throw new RuntimeException("块 " + i + " 同步至 " + dsIp + ":" + dsPort + " 失败");
                        }
                        System.out.println("[INFO] 块 " + i + " 同步至 " + dsIp + ":" + dsPort + " 成功");
                    }
                }

                // 同步MD5清单文件（新增）
                syncMd5ListToReplica(candidateDs, path, localMd5ListPath);

                // 添加副本节点至结果列表（最终共3个副本）
                for (Map<String, Object> ds : candidateDs) {
                    replicaLocations.add(ds.get("ip") + ":" + ds.get("port"));
                }
            } else {
                System.out.println("[INFO] 副本同步请求，仅写入本地，不触发进一步同步");
            }

            System.out.println("[INFO] 分块写入完成，三副本位置：" + replicaLocations);

            // 仅原始请求需要缓存副本位置
            if (!isReplicaSync) {
                // 缓存副本位置（key:文件路径，value:三副本位置）
                fileReplicaCache.put(path, replicaLocations);
                System.out.println("[INFO] 缓存文件副本位置：" + path + " -> " + replicaLocations);
            }

            return replicaLocations;
        } catch (Exception e) {
            System.err.println("[ERROR] 分块写入失败：" + e.getMessage());
            throw new RuntimeException("Write with chunk failed", e);
        }
    }
    /**
     * 分块写入文件（支持大文件），符合文档"三副本存储"和"MD5校验"要求
     * 附加详细日志用于排查写入失败问题
     * @param data 完整文件数据
     * @param path 文件路径（如/test/large_file.bin）
     * @return 包含三副本位置和块MD5的结果
     */

    /**
     * 分块读取文件并校验MD5
     */
    public byte[] readWithChunk(String path) {
        try {
            // 1. 读取本地MD5清单文件（替代从metaServer获取）
            String md5ListPath = path + "/md5_list.txt";
            String localMd5ListPath = getLocalFilePath(md5ListPath);
            List<String> expectedChunkMd5 = readMd5ListFromLocal(localMd5ListPath);
            if (expectedChunkMd5.isEmpty()) {
                throw new RuntimeException("MD5清单文件为空：" + localMd5ListPath);
            }

            // 2. 获取块文件并校验（原有逻辑不变）
            File chunkDir = new File(getLocalFilePath(path));
            if (!chunkDir.exists()) {
                throw new RuntimeException("File not found: " + path);
            }
            File[] chunkFiles = chunkDir.listFiles((dir, name) -> name.startsWith("chunk_"));
            if (chunkFiles == null || chunkFiles.length == 0) {
                throw new RuntimeException("No chunks found for: " + path);
            }
            // 校验块数量与MD5清单数量一致
            if (chunkFiles.length != expectedChunkMd5.size()) {
                throw new RuntimeException("块数量与MD5清单不匹配：块数" + chunkFiles.length + "，MD5数" + expectedChunkMd5.size());
            }

            // 3. 按块号排序并校验MD5
            List<File> sortedChunks = Arrays.stream(chunkFiles)
                    .sorted((f1, f2) -> {
                        int num1 = Integer.parseInt(f1.getName().split("_")[1]);
                        int num2 = Integer.parseInt(f2.getName().split("_")[1]);
                        return Integer.compare(num1, num2);
                    })
                    .collect(Collectors.toList());

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < sortedChunks.size(); i++) {
                File chunkFile = sortedChunks.get(i);
                byte[] chunkData = Files.readAllBytes(chunkFile.toPath());

                // 校验MD5（与清单中的预期值对比）
                String actualMd5 = DigestUtils.md5Hex(chunkData);
                String expectedMd5 = expectedChunkMd5.get(i);
                if (!actualMd5.equals(expectedMd5)) {
                    throw new RuntimeException("块 " + i + " MD5校验失败：预期" + expectedMd5 + "，实际" + actualMd5);
                }
                System.out.println("[INFO] 块 " + i + " MD5校验通过：" + actualMd5);

                bos.write(chunkData);
            }

            System.out.println("[INFO] 文件" + path + "读取完成，MD5校验通过");
            return bos.toByteArray();
        } catch (Exception e) {
            System.err.println("[ERROR] 分块读取失败：" + e.getMessage());
            throw new RuntimeException("Read with chunk failed", e);
        }
    }

    // 新增：从本地MD5清单文件读取预期MD5列表
    private List<String> readMd5ListFromLocal(String localMd5ListPath) throws Exception {
        List<String> expectedChunkMd5 = new ArrayList<>();
        File md5File = new File(localMd5ListPath);
        if (!md5File.exists()) {
            throw new RuntimeException("MD5清单文件不存在：" + localMd5ListPath);
        }

        // 按行读取，解析“块索引,MD5”格式
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(md5File))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split(",");
                if (parts.length != 2) {
                    throw new RuntimeException("MD5清单格式错误：" + line);
                }
                expectedChunkMd5.add(parts[1]); // 仅保留MD5值
            }
        }
        return expectedChunkMd5;
    }

    // 新增：将MD5列表写入本地清单文件
    public void writeMd5ListToLocal(String md5ListPath, byte[] md5ListData) throws IOException {
        // 步骤1：规范化路径，避免因路径格式错误导致父目录为null
        // （例如处理path为"/test/7.txt/md5_list.txt"的情况，确保能解析出父目录）
        File md5ListFile = new File(md5ListPath);

        // 关键校验：若父目录为null（路径格式错误），手动构造合理路径
        if (md5ListFile.getParentFile() == null) {
            // 假设本地存储根目录为localStoragePath（如D:\data\apps\minfs\dataserver\9002）
            // 拼接根目录与MD5清单路径，确保父目录存在
            md5ListFile = new File(localStoragePath, md5ListPath);
            System.out.println("[WARN] 路径格式异常，自动拼接根目录后路径：" + md5ListFile.getAbsolutePath());
        }

        // 步骤2：创建父目录（此时getParentFile()已非null）
        File parentDir = md5ListFile.getParentFile();
        if (!parentDir.exists()) {
            boolean dirCreated = parentDir.mkdirs();
            if (!dirCreated) {
                throw new IOException("创建MD5清单父目录失败：" + parentDir.getAbsolutePath());
            }
            System.out.println("[INFO] 创建MD5清单父目录：" + parentDir.getAbsolutePath());
        }

        // 直接用byte[]写入文件（无需List转换）
        try (FileOutputStream fos = new FileOutputStream(md5ListFile)) {
            fos.write(md5ListData);
        }
        System.out.println("[INFO] MD5清单写入成功：" + md5ListPath);
    }

    // 新增：同步MD5清单文件到副本节点
    private void syncMd5ListToReplica(List<Map<String, Object>> candidateDs, String path, String localMd5ListPath) throws Exception {
        byte[] md5ListData = Files.readAllBytes(new File(localMd5ListPath).toPath());
        // 步骤1：确保md5ListPath为相对路径（不含本地根目录），避免副本节点路径重复
        // （例如path为"/test/7.txt"，则md5ListPath为"test/7.txt/md5_list.txt"，去掉开头的"/"）
        String md5ListPath = path.startsWith("/") ? path.substring(1) : path;
        md5ListPath += "/md5_list.txt"; // 最终为"test/7.txt/md5_list.txt"

        for (Map<String, Object> ds : candidateDs) {
            String dsIp = (String) ds.get("ip");
            int dsPort = (int) ds.get("port");
            // 调用副本节点的“写入MD5清单”接口（需在副本节点Controller中实现）
            String url = "http://" + dsIp + ":" + dsPort + "/writeMd5List?path=" + URLEncoder.encode(md5ListPath, StandardCharsets.UTF_8.name());
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.set("X-Is-Replica-Sync", "true");
            headers.set("fileSystemName", "minfs");
            HttpEntity<byte[]> request = new HttpEntity<>(md5ListData, headers);

            try {
                ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
                if (!response.getStatusCode().is2xxSuccessful()) {
                    throw new RuntimeException("MD5清单同步至" + dsIp + ":" + dsPort + "失败，响应码：" + response.getStatusCodeValue());
                }
                System.out.println("[INFO] MD5清单同步至" + dsIp + ":" + dsPort + "成功");
            } catch (Exception e) {
                System.err.println("[ERROR] MD5清单同步至" + dsIp + ":" + dsPort + "失败：" + e.getMessage());
                throw e; // 传播异常，确保同步失败时整体写入操作失败
            }
        }
    }

    // 新增：检查目标节点文件是否存在（需在每个dataServer实现"文件存在"接口）
    private boolean checkFileExistsOnReplica(String dsIp, int dsPort, String path) {
        try {
            String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.name());
            String checkUrl = "http://" + dsIp + ":" + dsPort + "/checkFileExists?path=" + encodedPath;
            ResponseEntity<Boolean> response = restTemplate.getForEntity(checkUrl, Boolean.class);
            return response.getStatusCode().is2xxSuccessful() && Boolean.TRUE.equals(response.getBody());
        } catch (Exception e) {
            // 检查失败时默认继续同步（避免漏同步）
            return false;
        }
    }

    // 新增：提供获取缓存副本位置的方法
    public List<String> getCachedReplicaLocations(String path) {

        return fileReplicaCache.getOrDefault(path, new ArrayList<>());
    }

    // 新增：删除文件后清理缓存
    public void removeReplicaCache(String path) {
        fileReplicaCache.remove(path);
    }
}
