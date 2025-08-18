package com.example.dataserver.services;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DataService {

    @Value("${dataserver.storage.path}")
    private String localStoragePath; // 本地存储根目录（如/data/dataserver/storage）
    @Value("${dataserver.ip}")
    private String selfIp;
    @Value("${server.port}")
    private int selfPort;

    // 新增块大小常量（64MB）
    private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64MB

    /**
     * 生成本地存储路径（基于文件路径映射）
     */
    public String getLocalFilePath(String path) {
        try {
            // 先进行URL解码，处理MetaServer发送的编码路径
            String decodedPath = java.net.URLDecoder.decode(path, java.nio.charset.StandardCharsets.UTF_8.name());
            System.out.println("[DEBUG] getLocalFilePath 原始路径: '" + path + "'");
            System.out.println("[DEBUG] getLocalFilePath 解码后路径: '" + decodedPath + "'");
            
            // 例如：path为"/test/file.txt"，映射为localStoragePath/test/file.txt
            String localPath = localStoragePath + decodedPath.replace("/", File.separator);
            System.out.println("[DEBUG] getLocalFilePath 最终本地路径: '" + localPath + "'");
            return localPath;
        } catch (Exception e) {
            System.err.println("[ERROR] getLocalFilePath URL解码失败: " + path + ", 错误: " + e.getMessage());
            // 如果解码失败，使用原始路径
            return localStoragePath + path.replace("/", File.separator);
        }
    }

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

            // 不再由 DataServer 自行进行三副本同步；由 MetaServer 负责调度多副本写入
            // 这里无论是否副本同步请求，均只写入本地
            if (isReplicaSync) {
                System.out.println("[INFO] 副本同步请求，仅写入本地");
            } else {
                System.out.println("[INFO] 原始写入请求，由MetaServer负责三副本调度，本节点仅写入本地");
            }

            System.out.println("[INFO] 分块写入完成，三副本位置：" + replicaLocations);

            return replicaLocations;
        } catch (Exception e) {
            System.err.println("[ERROR] 分块写入失败：" + e.getMessage());
            throw new RuntimeException("Write with chunk failed", e);
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

    /**
     * 删除本地文件（包括分块目录和MD5清单）
     * @param path 文件路径
     * @return 是否删除成功
     */
    public boolean deleteFileLocally(String path) {
        try {
            String localFilePath = getLocalFilePath(path);
            File file = new File(localFilePath);
            System.out.println("[INFO] 开始删除本地文件：" + localFilePath);

            if (!file.exists()) {
                System.out.println("[WARN] 本地文件不存在，跳过删除：" + localFilePath);
                return true;
            }

            // 递归删除目录（文件分块存储在目录中）
            if (deleteDirectoryRecursively(file)) {
                System.out.println("[INFO] 本地文件删除成功：" + localFilePath);
                // 清理缓存
                return true;
            } else {
                System.err.println("[ERROR] 本地文件删除失败：" + localFilePath);
                return false;
            }
        } catch (Exception e) {
            System.err.println("[ERROR] 删除本地文件失败：" + e.getMessage());
            return false;
        }
    }

    /**
     * 递归删除目录及其中所有文件
     */
    private boolean deleteDirectoryRecursively(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    boolean success = deleteDirectoryRecursively(child);
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        // 目录为空或本身是文件，直接删除
        return dir.delete();
    }
}
