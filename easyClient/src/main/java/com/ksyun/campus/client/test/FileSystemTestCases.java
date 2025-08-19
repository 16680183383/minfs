package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.domain.ClusterInfo;

import java.util.List;

/**
 * 文件系统测试用例集合
 * 提供完整的测试场景，用于验证easyClient SDK功能
 */
public class FileSystemTestCases {
    
    private EFileSystem fileSystem;
    
    public FileSystemTestCases(EFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
    
    /**
     * 测试用例1: 基础目录操作
     * 测试创建、删除目录功能
     */
    public void testCase1_BasicDirectoryOperations() {
        System.out.println("=== 测试用例1: 基础目录操作 ===");
        
        try {
            // 1.1 创建目录
            System.out.println("1.1 创建目录 /test_basic");
            boolean mkdirResult = fileSystem.mkdir("/test_basic");
            System.out.println("   结果: " + mkdirResult);
            
            // 1.2 创建多级目录
            System.out.println("1.2 创建多级目录 /test_basic/sub1/sub2");
            boolean mkdirSubResult = fileSystem.mkdir("/test_basic/sub1/sub2");
            System.out.println("   结果: " + mkdirSubResult);
            
            // 1.3 检查目录是否存在
            System.out.println("1.3 检查目录是否存在");
            boolean exists = fileSystem.exists("/test_basic");
            System.out.println("   目录存在: " + exists);
            
            // 1.4 列出目录内容
            System.out.println("1.4 列出目录内容");
            List<StatInfo> files = fileSystem.listFileStats("/test_basic");
            if (files != null) {
                System.out.println("   目录包含 " + files.size() + " 个条目:");
                for (StatInfo file : files) {
                    System.out.println("     - " + file.getPath() + " (类型: " + file.getType() + ")");
                }
            }
            
            // 1.5 清理测试目录
            System.out.println("1.5 清理测试目录");
            boolean deleteResult = fileSystem.delete("/test_basic");
            System.out.println("   删除结果: " + deleteResult);
            
        } catch (Exception e) {
            System.err.println("测试用例1执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例1完成 ===\n");
    }
    
    /**
     * 测试用例2: 基础文件操作
     * 测试创建、写入、读取、删除文件功能
     */
    public void testCase2_BasicFileOperations() {
        System.out.println("=== 测试用例2: 基础文件操作 ===");
        
        try {
            // 2.1 创建目录
            System.out.println("2.1 创建测试目录");
            fileSystem.mkdir("/test_file_ops");
            
            // 2.2 创建文件
            System.out.println("2.2 创建文件 /test_file_ops/hello.txt");
            FSOutputStream outputStream = fileSystem.create("/test_file_ops/hello.txt");
            if (outputStream != null) {
                String content = "Hello, easyClient SDK! 这是一个测试文件。\n包含中文内容：你好世界！";
                outputStream.write(content.getBytes("UTF-8"));
                outputStream.close();
                System.out.println("   文件创建成功，内容长度: " + content.length() + " 字节");
            }
            
            // 2.3 检查文件是否存在
            System.out.println("2.3 检查文件是否存在");
            boolean exists = fileSystem.exists("/test_file_ops/hello.txt");
            System.out.println("   文件存在: " + exists);
            
            // 2.4 获取文件状态
            System.out.println("2.4 获取文件状态");
            StatInfo statInfo = fileSystem.getFileStats("/test_file_ops/hello.txt");
            if (statInfo != null) {
                System.out.println("   文件路径: " + statInfo.getPath());
                System.out.println("   文件大小: " + statInfo.getSize() + " 字节");
                System.out.println("   文件类型: " + statInfo.getType());
                System.out.println("   修改时间: " + statInfo.getMtime());
            }
            
            // 2.5 读取文件内容
            System.out.println("2.5 读取文件内容");
            FSInputStream inputStream = fileSystem.open("/test_file_ops/hello.txt");
            if (inputStream != null) {
                byte[] buffer = new byte[1024];
                int bytesRead = inputStream.read(buffer);
                if (bytesRead > 0) {
                    String readContent = new String(buffer, 0, bytesRead, "UTF-8");
                    System.out.println("   读取内容: " + readContent);
                }
                inputStream.close();
            }
            
            // 2.6 清理测试文件
            System.out.println("2.6 清理测试文件");
            boolean deleteFile = fileSystem.delete("/test_file_ops/hello.txt");
            boolean deleteDir = fileSystem.delete("/test_file_ops");
            System.out.println("   删除文件: " + deleteFile + ", 删除目录: " + deleteDir);
            
        } catch (Exception e) {
            System.err.println("测试用例2执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例2完成 ===\n");
    }
    
    /**
     * 测试用例3: 文件写入和读取
     * 测试大文件写入、分块读取等功能
     */
    public void testCase3_FileWriteAndRead() {
        System.out.println("=== 测试用例3: 文件写入和读取 ===");
        
        try {
            // 3.1 创建目录
            System.out.println("3.1 创建测试目录");
            fileSystem.mkdir("/test_write_read");
            
            // 3.2 创建大文件
            System.out.println("3.2 创建大文件 /test_write_read/bigfile.txt");
            FSOutputStream outputStream = fileSystem.create("/test_write_read/bigfile.txt");
            if (outputStream != null) {
                // 写入多行内容
                StringBuilder content = new StringBuilder();
                for (int i = 1; i <= 100; i++) {
                    content.append("第").append(i).append("行: 这是测试内容，包含数字").append(i).append("\n");
                }
                
                byte[] data = content.toString().getBytes("UTF-8");
                outputStream.write(data);
                outputStream.close();
                System.out.println("   文件创建成功，内容长度: " + data.length + " 字节");
            }
            
            // 3.3 分块读取文件
            System.out.println("3.3 分块读取文件");
            FSInputStream inputStream = fileSystem.open("/test_write_read/bigfile.txt");
            if (inputStream != null) {
                byte[] buffer = new byte[256]; // 每次读取256字节
                int totalRead = 0;
                int chunkCount = 0;
                
                while (true) {
                    int bytesRead = inputStream.read(buffer);
                    if (bytesRead <= 0) break;
                    
                    chunkCount++;
                    totalRead += bytesRead;
                    
                    if (chunkCount <= 3) { // 只显示前3块内容
                        String chunk = new String(buffer, 0, bytesRead, "UTF-8");
                        System.out.println("   第" + chunkCount + "块 (" + bytesRead + "字节): " + chunk.substring(0, Math.min(50, chunk.length())) + "...");
                    }
                }
                
                System.out.println("   总共读取: " + totalRead + " 字节，分 " + chunkCount + " 块");
                inputStream.close();
            }
            
            // 3.4 清理测试文件
            System.out.println("3.4 清理测试文件");
            boolean deleteFile = fileSystem.delete("/test_write_read/bigfile.txt");
            boolean deleteDir = fileSystem.delete("/test_write_read");
            System.out.println("   删除文件: " + deleteFile + ", 删除目录: " + deleteDir);
            
        } catch (Exception e) {
            System.err.println("测试用例3执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例3完成 ===\n");
    }
    
    /**
     * 测试用例4: 集群信息获取
     * 测试获取集群状态、DataServer信息等
     */
    public void testCase4_ClusterInformation() {
        System.out.println("=== 测试用例4: 集群信息获取 ===");
        
        try {
            // 4.1 获取集群信息
            System.out.println("4.1 获取集群信息");
            ClusterInfo clusterInfo = fileSystem.getClusterInfo();
            if (clusterInfo != null) {
                System.out.println("   集群信息获取成功");
                System.out.println("   主MetaServer: " + clusterInfo.getMasterMetaServer());
                System.out.println("   从MetaServer: " + clusterInfo.getSlaveMetaServer());
                
                if (clusterInfo.getDataServer() != null) {
                    System.out.println("   DataServer数量: " + clusterInfo.getDataServer().size());
                    for (int i = 0; i < Math.min(3, clusterInfo.getDataServer().size()); i++) {
                        var ds = clusterInfo.getDataServer().get(i);
                        System.out.println("     DataServer " + (i+1) + ": " + ds.getHost() + ":" + ds.getPort());
                    }
                }
            } else {
                System.out.println("   集群信息获取失败");
            }
            
        } catch (Exception e) {
            System.err.println("测试用例4执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例4完成 ===\n");
    }
    
    /**
     * 测试用例5: 错误处理和边界情况
     * 测试各种异常情况的处理
     */
    public void testCase5_ErrorHandling() {
        System.out.println("=== 测试用例5: 错误处理和边界情况 ===");
        
        try {
            // 5.1 测试访问不存在的文件
            System.out.println("5.1 测试访问不存在的文件");
            StatInfo nonExistentFile = fileSystem.getFileStats("/non/existent/file.txt");
            System.out.println("   不存在的文件状态: " + (nonExistentFile == null ? "null" : "非null"));
            
            // 5.2 测试访问不存在的目录
            System.out.println("5.2 测试访问不存在的目录");
            List<StatInfo> nonExistentDir = fileSystem.listFileStats("/non/existent/directory");
            System.out.println("   不存在的目录列表: " + (nonExistentDir == null ? "null" : "非null"));
            
            // 5.3 测试删除不存在的文件
            System.out.println("5.3 测试删除不存在的文件");
            boolean deleteNonExistent = fileSystem.delete("/non/existent/file.txt");
            System.out.println("   删除不存在文件结果: " + deleteNonExistent);
            
            // 5.4 测试空路径
            System.out.println("5.4 测试空路径");
            try {
                fileSystem.mkdir("");
                System.out.println("   空路径创建目录成功");
            } catch (Exception e) {
                System.out.println("   空路径创建目录失败: " + e.getMessage());
            }
            
        } catch (Exception e) {
            System.err.println("测试用例5执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例5完成 ===\n");
    }
    
    /**
     * 测试用例6: 性能测试
     * 测试文件操作的性能表现
     */
    public void testCase6_PerformanceTest() {
        System.out.println("=== 测试用例6: 性能测试 ===");
        
        try {
            // 6.1 创建目录
            System.out.println("6.1 创建测试目录");
            fileSystem.mkdir("/test_performance");
            
            // 6.2 批量创建文件
            System.out.println("6.2 批量创建文件 (10个)");
            long startTime = System.currentTimeMillis();
            
            for (int i = 1; i <= 10; i++) {
                String fileName = "/test_performance/file" + i + ".txt";
                FSOutputStream outputStream = fileSystem.create(fileName);
                if (outputStream != null) {
                    String content = "这是第" + i + "个测试文件，内容长度适中。";
                    outputStream.write(content.getBytes("UTF-8"));
                    outputStream.close();
                }
            }
            
            long createTime = System.currentTimeMillis() - startTime;
            System.out.println("   批量创建文件耗时: " + createTime + " 毫秒");
            
            // 6.3 批量读取文件
            System.out.println("6.3 批量读取文件");
            startTime = System.currentTimeMillis();
            
            for (int i = 1; i <= 10; i++) {
                String fileName = "/test_performance/file" + i + ".txt";
                FSInputStream inputStream = fileSystem.open(fileName);
                if (inputStream != null) {
                    byte[] buffer = new byte[1024];
                    inputStream.read(buffer);
                    inputStream.close();
                }
            }
            
            long readTime = System.currentTimeMillis() - startTime;
            System.out.println("   批量读取文件耗时: " + readTime + " 毫秒");
            
            // 6.4 清理测试文件
            System.out.println("6.4 清理测试文件");
            for (int i = 1; i <= 10; i++) {
                String fileName = "/test_performance/file" + i + ".txt";
                fileSystem.delete(fileName);
            }
            fileSystem.delete("/test_performance");
            
        } catch (Exception e) {
            System.err.println("测试用例6执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("=== 测试用例6完成 ===\n");
    }
    
    /**
     * 运行所有测试用例
     */
    public void runAllTestCases() {
        System.out.println("🚀 开始运行所有测试用例...\n");
        
        testCase1_BasicDirectoryOperations();
        testCase2_BasicFileOperations();
        testCase3_FileWriteAndRead();
        testCase4_ClusterInformation();
        testCase5_ErrorHandling();
        testCase6_PerformanceTest();
        
        System.out.println("✅ 所有测试用例执行完成！");
    }
}
