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
    
    @Autowired
    private ZkService zkService;

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
     * 分块读取文件内容
     * @param path 文件路径
     * @param offset 偏移量（字节）
     * @param length 读取长度（字节），-1表示读取到文件末尾
     */
    public byte[] readWithChunk(String path, int offset, int length) {
        try {
            // 1. 读取本地MD5清单文件（替代从metaServer获取）
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(md5ListPath);
            List<String> expectedChunkMd5 = readMd5ListFromLocal(localMd5ListPath);
            if (expectedChunkMd5.isEmpty()) {
                throw new RuntimeException("MD5清单文件为空：" + localMd5ListPath);
            }

            // 2. 获取块文件并校验（修复路径处理）
            List<File> chunkFiles = new ArrayList<>();
            for (int i = 0; i < expectedChunkMd5.size(); i++) {
                String chunkPath = path + "_chunk_" + i;
                File chunkFile = new File(getLocalFilePath(chunkPath));
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
            System.out.println("[INFO] 文件" + path + "读取完成，MD5校验通过，总大小：" + fullData.length + "字节");
            
            // 处理分块读取
            if (offset > 0 || length > 0) {
                if (offset >= fullData.length) {
                    System.out.println("[WARN] 偏移量超出文件大小: offset=" + offset + ", fileSize=" + fullData.length);
                    return new byte[0];
                }
                
                int actualLength = length > 0 ? length : fullData.length - offset;
                int endPos = Math.min(offset + actualLength, fullData.length);
                int resultLength = endPos - offset;
                
                byte[] result = new byte[resultLength];
                System.arraycopy(fullData, offset, result, 0, resultLength);
                
                System.out.println("[INFO] 分块读取成功: path=" + path + ", offset=" + offset + 
                                 ", length=" + actualLength + ", actualLength=" + resultLength);
                return result;
            }
            
            return fullData;
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

                // 修复：文件应该直接存储在path路径下，而不是在path下创建子目录
                String chunkPath = path + "_chunk_" + i;
                writeToLocal(getLocalFilePath(chunkPath), chunk, 0);
            }

            // 2. 生成MD5清单文件（与分块文件同目录，不依赖metaServer）
            String md5ListPath = path + "_md5_list.txt";
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

            // 更新ZK中的已使用容量
            long totalDataSize = data.length + md5ListBytes.length;
            zkService.addUsedCapacity(totalDataSize);
            System.out.println("[INFO] 容量更新: +" + totalDataSize + " 字节");

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
     * 删除本地文件（包括分块文件和MD5清单）
     * @param path 文件路径
     * @return 是否删除成功
     */
    public boolean deleteFileLocally(String path) {
        try {
            System.out.println("[INFO] 开始删除本地文件：" + path);
            
            // 计算要删除的文件总大小
            long totalDeletedSize = calculateFileSize(path);

            // 删除分块文件
            boolean chunksDeleted = deleteChunkFiles(path);
            
            // 删除MD5清单文件
            boolean md5ListDeleted = deleteMd5ListFile(path);
            
            if (chunksDeleted && md5ListDeleted) {
                // 更新ZK中的已使用容量
                zkService.subtractUsedCapacity(totalDeletedSize);
                System.out.println("[INFO] 容量更新: -" + totalDeletedSize + " 字节");
                
                System.out.println("[INFO] 本地文件删除成功：" + path);
                return true;
            } else {
                System.err.println("[ERROR] 本地文件删除失败：" + path + 
                                 ", chunksDeleted=" + chunksDeleted + 
                                 ", md5ListDeleted=" + md5ListDeleted);
                return false;
            }
        } catch (Exception e) {
            System.err.println("[ERROR] 删除本地文件失败：" + e.getMessage());
            return false;
        }
    }
    
    /**
     * 删除分块文件
     */
    private boolean deleteChunkFiles(String path) {
        try {
            boolean allDeleted = true;
            int chunkIndex = 0;
            
            while (true) {
                String chunkPath = path + "_chunk_" + chunkIndex;
                String localChunkPath = getLocalFilePath(chunkPath);
                File chunkFile = new File(localChunkPath);
                
                if (!chunkFile.exists()) {
                    break; // 没有更多分块文件
                }
                
                boolean deleted = chunkFile.delete();
                if (deleted) {
                    System.out.println("[INFO] 删除分块文件成功：" + chunkPath);
                } else {
                    System.err.println("[ERROR] 删除分块文件失败：" + chunkPath);
                    allDeleted = false;
                }
                
                chunkIndex++;
            }
            
            return allDeleted;
        } catch (Exception e) {
            System.err.println("[ERROR] 删除分块文件失败：" + e.getMessage());
            return false;
        }
    }
    
    /**
     * 删除MD5清单文件
     */
    private boolean deleteMd5ListFile(String path) {
        try {
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(md5ListPath);
            File md5ListFile = new File(localMd5ListPath);
            
            if (!md5ListFile.exists()) {
                System.out.println("[INFO] MD5清单文件不存在，跳过删除：" + md5ListPath);
                return true;
            }
            
            boolean deleted = md5ListFile.delete();
            if (deleted) {
                System.out.println("[INFO] 删除MD5清单文件成功：" + md5ListPath);
            } else {
                System.err.println("[ERROR] 删除MD5清单文件失败：" + md5ListPath);
            }
            
            return deleted;
        } catch (Exception e) {
            System.err.println("[ERROR] 删除MD5清单文件失败：" + e.getMessage());
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
    private long calculateFileSize(String path) {
        long totalSize = 0;
        try {
            // 计算MD5清单文件大小
            String md5ListPath = path + "_md5_list.txt";
            String localMd5ListPath = getLocalFilePath(md5ListPath);
            File md5ListFile = new File(localMd5ListPath);
            if (md5ListFile.exists()) {
                totalSize += md5ListFile.length();
            }

            // 计算分块文件大小
            int chunkIndex = 0;
            while (true) {
                String chunkPath = path + "_chunk_" + chunkIndex;
                String localChunkPath = getLocalFilePath(chunkPath);
                File chunkFile = new File(localChunkPath);
                
                if (!chunkFile.exists()) {
                    break;
                }
                totalSize += chunkFile.length();
                chunkIndex++;
            }
        } catch (Exception e) {
            System.err.println("[ERROR] 计算文件大小失败：" + e.getMessage());
            return 0;
        }
        return totalSize;
    }
}
