package com.example.dataserver.services;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DataService {

    @Value("${dataserver.storage.path:D:/data/apps/minfs/dataserver}")
    private String localStoragePath; // 本地存储根目录（如/data/dataserver/storage）
    @Value("${dataserver.ip}")
    private String selfIp;
    @Value("${server.port}")
    private int selfPort;
    
    @Autowired
    private ZkService zkService;

    // 新增块大小常量（64MB）
    private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64MB

    /**
     * 生成本地存储路径（基于文件系统名称和文件路径映射）
     */
    public String getLocalFilePath(String fileSystemName, String path) {
        try {
            // 先进行URL解码，处理MetaServer发送的编码路径
            String decodedPath = java.net.URLDecoder.decode(path, java.nio.charset.StandardCharsets.UTF_8.name());
            System.out.println("[DEBUG] getLocalFilePath 文件系统: " + fileSystemName + ", 原始路径: '" + path + "'");
            System.out.println("[DEBUG] getLocalFilePath 文件系统: " + fileSystemName + ", 解码后路径: '" + decodedPath + "'");
            
            // 例如：fileSystemName为"fs1"，path为"/test/file.txt"，映射为localStoragePath/fs1/test/file.txt
            String localPath = localStoragePath + File.separator + fileSystemName + decodedPath.replace("/", File.separator);
            System.out.println("[DEBUG] getLocalFilePath 文件系统: " + fileSystemName + ", 最终本地路径: '" + localPath + "'");
            return localPath;
        } catch (Exception e) {
            System.err.println("[ERROR] getLocalFilePath URL解码失败: fileSystemName=" + fileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            // 如果解码失败，使用原始路径
            return localStoragePath + File.separator + fileSystemName + path.replace("/", File.separator);
        }
    }

    /**
     * 分块读取文件内容
     * @param fileSystemName 文件系统名称
     * @param path 文件路径
     * @param offset 偏移量（字节）
     * @param length 读取长度（字节），-1表示读取到文件末尾
     */
    public byte[] readWithChunk(String fileSystemName, String path, int offset, int length) {
        try {
            // 1. 读取本地MD5清单文件（替代从metaServer获取）
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(fileSystemName, md5ListPath);
            List<String> expectedChunkMd5 = readMd5ListFromLocal(localMd5ListPath);
            if (expectedChunkMd5.isEmpty()) {
                throw new RuntimeException("MD5清单文件为空：" + localMd5ListPath);
            }

            // 2. 获取块文件并校验（修复路径处理）
            List<File> chunkFiles = new ArrayList<>();
            for (int i = 0; i < expectedChunkMd5.size(); i++) {
                String chunkPath = path + "_chunk_" + i;
                File chunkFile = new File(getLocalFilePath(fileSystemName, chunkPath));
                if (!chunkFile.exists()) {
                    throw new RuntimeException("块文件不存在：" + chunkPath);
                }
                chunkFiles.add(chunkFile);
            }
            
            if (chunkFiles.isEmpty()) {
                throw new RuntimeException("No chunks found for: " + path);
            }
            
            // 校验块数量与MD5清单数量一致
            if (chunkFiles.size() != expectedChunkMd5.size()) {
                throw new RuntimeException("块数量与MD5清单不匹配：块数" + chunkFiles.size() + "，MD5数" + expectedChunkMd5.size());
            }

            // 3. 按块号排序并校验MD5
            List<File> sortedChunks = chunkFiles.stream()
                    .sorted((f1, f2) -> {
                        int num1 = Integer.parseInt(f1.getName().split("_chunk_")[1]);
                        int num2 = Integer.parseInt(f2.getName().split("_chunk_")[1]);
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

            byte[] fullData = bos.toByteArray();
            System.out.println("[INFO] 文件系统: " + fileSystemName + ", 文件" + path + "读取完成，MD5校验通过，总大小：" + fullData.length + "字节");
            
            // 处理分块读取
            if (offset > 0 || length > 0) {
                if (offset >= fullData.length) {
                    System.out.println("[WARN] 偏移量超出文件大小: fileSystemName=" + fileSystemName + ", path=" + path + ", offset=" + offset + ", fileSize=" + fullData.length);
                    return new byte[0];
                }
                
                int actualLength = length > 0 ? length : fullData.length - offset;
                int endPos = Math.min(offset + actualLength, fullData.length);
                int resultLength = endPos - offset;
                
                byte[] result = new byte[resultLength];
                System.arraycopy(fullData, offset, result, 0, resultLength);
                
                System.out.println("[INFO] 分块读取成功: fileSystemName=" + fileSystemName + ", path=" + path + ", offset=" + offset + ", length=" + length + ", actualLength=" + resultLength);
                return result;
            }
            
            return fullData;
        } catch (Exception e) {
            System.err.println("[ERROR] 读取文件失败: fileSystemName=" + fileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new RuntimeException("读取文件失败: " + e.getMessage(), e);
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
     * 分块写入文件内容
     * @param fileSystemName 文件系统名称
     * @param data 要写入的数据
     * @param path 文件路径
     * @param isReplicaSync 是否为副本同步请求
     * @return 副本位置列表
     */
    public List<String> writeWithChunk(String fileSystemName, byte[] data, String path, boolean isReplicaSync) {
        try {
            List<String> replicaLocations = new ArrayList<>();
            replicaLocations.add(selfIp + ":" + selfPort); // 本节点始终作为副本之一

            System.out.println("[INFO] 分块写入开始，文件系统: " + fileSystemName + ", 目标路径：" + path + "，本节点：" + selfIp + ":" + selfPort + "，是否为副本同步请求：" + isReplicaSync);

            // 1. 数据分块
            int totalChunks = (int) Math.ceil((double) data.length / BLOCK_SIZE);
            System.out.println("[INFO] 数据分块：" + totalChunks + "块，块大小：" + BLOCK_SIZE + "字节");

            List<String> chunkMd5List = new ArrayList<>();
            
            // 2. 写入本地分块文件
            for (int i = 0; i < totalChunks; i++) {
                int start = i * BLOCK_SIZE;
                int length = Math.min(BLOCK_SIZE, data.length - start);
                System.out.println("[INFO] 处理块 " + i + "，大小：" + length + "字节");

                byte[] chunkData = new byte[length];
                System.arraycopy(data, start, chunkData, 0, length);
                
                String chunkMd5 = DigestUtils.md5Hex(chunkData);
                chunkMd5List.add(chunkMd5);
                System.out.println("[INFO] 块 " + i + " MD5：" + chunkMd5);

                String chunkPath = path + "_chunk_" + i;
                String localChunkPath = getLocalFilePath(fileSystemName, chunkPath);
                java.nio.file.Path chunkFilePath = Paths.get(localChunkPath);
                // 确保父目录存在
                java.nio.file.Path parentDir = chunkFilePath.getParent();
                if (parentDir != null && !java.nio.file.Files.exists(parentDir)) {
                    java.nio.file.Files.createDirectories(parentDir);
                }
                Files.write(chunkFilePath, chunkData);
            }

            // 3. 生成MD5清单文件
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(fileSystemName, md5ListPath);
            String md5ListContent = String.join("\n", chunkMd5List);
            writeToLocal(localMd5ListPath, md5ListContent.getBytes(StandardCharsets.UTF_8), 0);
            System.out.println("[INFO] MD5清单文件已生成：" + localMd5ListPath + "，包含" + chunkMd5List.size() + "个MD5值");

            // 4. 如果不是副本同步请求，则尝试同步到其他DataServer（简化实现）
            if (!isReplicaSync) {
                System.out.println("[INFO] 非副本同步请求，简化副本同步逻辑");
                // 这里可以添加获取其他DataServer地址的逻辑
                // 暂时跳过，实际实现时需要从ZK获取其他DataServer地址
            } else {
                System.out.println("[INFO] 副本同步请求，跳过同步到其他DataServer");
            }

            // 5. 返回副本位置列表
            System.out.println("[INFO] 分块写入完成，文件系统: " + fileSystemName + ", 路径：" + path + "，副本位置：" + replicaLocations);
            return replicaLocations;

        } catch (Exception e) {
            System.err.println("[ERROR] 分块写入失败：fileSystemName=" + fileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new RuntimeException("Write with chunk failed", e);
        }
    }

    /**
     * 同步数据到其他DataServer
     */
    private boolean syncToOtherDataServer(String fileSystemName, String otherServer, String path, byte[] data) {
        try {
            String url = "http://" + otherServer + "/write";
            
            // 构建请求参数
            String queryParams = String.format("?path=%s&offset=0&length=%d", path, data.length);
            
            // 设置请求头
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.set("fileSystemName", fileSystemName);
            headers.set("X-Is-Replica-Sync", "true");
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM);
            
            // 发送请求
            org.springframework.http.HttpEntity<byte[]> entity = new org.springframework.http.HttpEntity<>(data, headers);
            org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();
            org.springframework.http.ResponseEntity<String> response = restTemplate.exchange(
                url + queryParams,
                org.springframework.http.HttpMethod.POST,
                entity,
                String.class
            );
            
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            System.err.println("[ERROR] 同步到其他DataServer失败：" + otherServer + "，错误：" + e.getMessage());
            return false;
        }
    }

    /**
     * 从本地读取MD5清单文件
     */
    private List<String> readMd5ListFromLocal(String localMd5ListPath) {
        try {
            File md5ListFile = new File(localMd5ListPath);
            if (!md5ListFile.exists()) {
                System.err.println("[ERROR] MD5清单文件不存在：" + localMd5ListPath);
                return new ArrayList<>();
            }
            
            List<String> md5List = Files.readAllLines(md5ListFile.toPath(), StandardCharsets.UTF_8);
            System.out.println("[INFO] 成功读取MD5清单文件：" + localMd5ListPath + "，包含" + md5List.size() + "个MD5值");
            return md5List;
        } catch (Exception e) {
            System.err.println("[ERROR] 读取MD5清单文件失败：" + localMd5ListPath + "，错误：" + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * 写入MD5清单到本地
     * @param fileSystemName 文件系统名称
     * @param path MD5清单路径
     * @param md5ListData MD5清单数据
     * @param isReplicaSync 是否为副本同步请求
     * @return 是否写入成功
     */
    public boolean writeMd5List(String fileSystemName, String path, byte[] md5ListData, boolean isReplicaSync) {
        try {
            System.out.println("[INFO] 写入MD5清单，文件系统: " + fileSystemName + ", 路径: " + path + ", 数据大小: " + md5ListData.length + "字节, 副本同步: " + isReplicaSync);
            
            // 写入MD5清单文件
            String localMd5ListPath = getLocalFilePath(fileSystemName, path);
            writeToLocal(localMd5ListPath, md5ListData, 0);
            
            System.out.println("[INFO] MD5清单写入成功，文件系统: " + fileSystemName + ", 路径: " + path);
                return true;
        } catch (Exception e) {
            System.err.println("[ERROR] MD5清单写入失败，文件系统: " + fileSystemName + ", 路径: " + path + ", 错误: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 删除本地文件
     * @param fileSystemName 文件系统名称
     * @param path 文件路径
     * @return 是否删除成功
     */
    public boolean deleteFileLocally(String fileSystemName, String path) {
        try {
            System.out.println("[INFO] 删除本地文件，文件系统: " + fileSystemName + ", 路径: " + path);
            
            // 删除分块文件
            int chunkIndex = 0;
            while (true) {
                String chunkPath = path + "_chunk_" + chunkIndex;
                String localChunkPath = getLocalFilePath(fileSystemName, chunkPath);
                File chunkFile = new File(localChunkPath);
                
                if (!chunkFile.exists()) {
                    break;
                }
                
                if (chunkFile.delete()) {
                    System.out.println("[INFO] 删除分块文件成功：" + localChunkPath);
                } else {
                    System.err.println("[ERROR] 删除分块文件失败：" + localChunkPath);
                }
                
                chunkIndex++;
            }
            
            // 删除MD5清单文件
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(fileSystemName, md5ListPath);
            File md5ListFile = new File(localMd5ListPath);
            if (md5ListFile.exists()) {
                if (md5ListFile.delete()) {
                    System.out.println("[INFO] 删除MD5清单文件成功：" + localMd5ListPath);
                } else {
                    System.err.println("[ERROR] 删除MD5清单文件失败：" + localMd5ListPath);
                }
            }
            
            System.out.println("[INFO] 本地文件删除完成，文件系统: " + fileSystemName + ", 路径: " + path);
                return true;
        } catch (Exception e) {
            System.err.println("[ERROR] 删除本地文件失败，文件系统: " + fileSystemName + ", 路径: " + path + ", 错误: " + e.getMessage());
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

    /**
     * 计算文件大小（包括分块文件和MD5清单）
     */
    private long calculateFileSize(String fileSystemName, String path) {
        long totalSize = 0;
        try {
            // 计算MD5清单文件大小
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(fileSystemName, md5ListPath);
            File md5ListFile = new File(localMd5ListPath);
            if (md5ListFile.exists()) {
                totalSize += md5ListFile.length();
            }

            // 计算分块文件大小
            int chunkIndex = 0;
            while (true) {
                String chunkPath = path + "_chunk_" + chunkIndex;
                String localChunkPath = getLocalFilePath(fileSystemName, chunkPath);
                File chunkFile = new File(localChunkPath);
                
                if (!chunkFile.exists()) {
                    break;
                }
                totalSize += chunkFile.length();
                chunkIndex++;
            }
        } catch (Exception e) {
            System.err.println("[ERROR] 计算文件大小失败：fileSystemName=" + fileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            return 0;
        }
        return totalSize;
    }
}
