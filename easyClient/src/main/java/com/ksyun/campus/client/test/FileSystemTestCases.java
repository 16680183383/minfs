package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.domain.ClusterInfo;

import java.util.List;
import java.util.Map;

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
            System.out.println("   [开始] 删除文件 /test_file_ops/hello.txt");
            boolean deleteFile = fileSystem.delete("/test_file_ops/hello.txt");
            System.out.println("   [结束] 删除文件结果: " + deleteFile);
            
            // 验证文件删除后元数据也被清理
            System.out.println("   [验证] 检查删除后的文件状态");
            try {
                StatInfo deletedFileStats = fileSystem.getFileStats("/test_file_ops/hello.txt");
                if (deletedFileStats == null) {
                    System.out.println("   [成功] 文件元数据已正确删除");
                } else {
                    System.out.println("   [警告] 文件元数据仍然存在: " + deletedFileStats.getPath());
                }
            } catch (Exception e) {
                System.out.println("   [成功] 文件元数据访问失败，说明已删除: " + e.getMessage());
            }
            
            System.out.println("   [开始] 删除目录 /test_file_ops");
            boolean deleteDir = fileSystem.delete("/test_file_ops");
            System.out.println("   [结束] 删除目录结果: " + deleteDir);
            
            // 验证目录删除后元数据也被清理
            System.out.println("   [验证] 检查删除后的目录状态");
            try {
                List<StatInfo> deletedDirStats = fileSystem.listFileStats("/test_file_ops");
                if (deletedDirStats == null) {
                    System.out.println("   [成功] 目录元数据已正确删除");
                } else {
                    System.out.println("   [警告] 目录元数据仍然存在，包含 " + deletedDirStats.size() + " 个子项");
                }
            } catch (Exception e) {
                System.out.println("   [成功] 目录元数据访问失败，说明已删除: " + e.getMessage());
            }
            
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

            testCase4_ClusterInformation();

            // 3.4 清理测试文件
            System.out.println("3.4 清理测试文件");
            System.out.println("   [开始] 删除文件 /test_write_read/bigfile.txt");
            boolean deleteFile = fileSystem.delete("/test_write_read/bigfile.txt");
            System.out.println("   [结束] 删除文件结果: " + deleteFile);
            
            // 验证文件删除后元数据也被清理
            System.out.println("   [验证] 检查删除后的文件状态");
            try {
                StatInfo deletedFileStats = fileSystem.getFileStats("/test_write_read/bigfile.txt");
                if (deletedFileStats == null) {
                    System.out.println("   [成功] 文件元数据已正确删除");
                } else {
                    System.out.println("   [警告] 文件元数据仍然存在: " + deletedFileStats.getPath());
                }
            } catch (Exception e) {
                System.out.println("   [成功] 文件元数据访问失败，说明已删除: " + e.getMessage());
            }
            
            System.out.println("   [开始] 删除目录 /test_write_read");
            boolean deleteDir = fileSystem.delete("/test_write_read");
            System.out.println("   [结束] 删除目录结果: " + deleteDir);

            // 验证目录删除后元数据也被清理
            System.out.println("   [验证] 检查删除后的目录状态");
            try {
                List<StatInfo> deletedDirStats = fileSystem.listFileStats("/test_write_read");
                if (deletedDirStats == null) {
                    System.out.println("   [成功] 目录元数据已正确删除");
                } else {
                    System.out.println("   [警告] 目录元数据仍然存在，包含 " + deletedDirStats.size() + " 个子项");
                }
            } catch (Exception e) {
                System.out.println("   [成功] 目录元数据访问失败，说明已删除: " + e.getMessage());
            }

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
                
                // 检查是否有错误
                if (clusterInfo.getError() != null) {
                    System.err.println("   集群信息获取错误: " + clusterInfo.getError());
                    return;
                }
                
                // 4.1 MetaServer集群信息:
                System.out.println("4.1 MetaServer集群信息:");
                if (clusterInfo.getMetaServers() != null) {
                    Map<String, Object> metaServers = clusterInfo.getMetaServers();
                    System.out.println("   Leader地址: " + metaServers.get("leaderAddress"));
                    System.out.println("   Follower地址列表: " + metaServers.get("followerAddresses"));
                }
                
                // 4.2 显示MetaServer集群信息
                System.out.println("4.2 MetaServer集群信息");
                if (clusterInfo.getMetaServers() != null) {
                    Map<String, Object> metaServers = clusterInfo.getMetaServers();
                    System.out.println("   Leader地址: " + metaServers.get("leaderAddress"));
                    System.out.println("   Follower地址列表: " + metaServers.get("followerAddresses"));
                } else {
                    System.out.println("   MetaServer信息为空");
                }
                
                // 4.3 显示DataServer集群信息
                System.out.println("4.3 DataServer集群信息");
                if (clusterInfo.getDataServers() != null) {
                    System.out.println("   总DataServer数量: " + clusterInfo.getTotalDataServers());
                    System.out.println("   活跃DataServer数量: " + clusterInfo.getActiveDataServers());
                    
                    List<Map<String, Object>> dataServers = clusterInfo.getDataServers();
                    System.out.println("   DataServer详细信息:");
                    for (int i = 0; i < Math.min(5, dataServers.size()); i++) {
                        Map<String, Object> ds = dataServers.get(i);
                        System.out.println("     DataServer " + (i+1) + ": " + ds.get("address") + 
                                         " (容量: " + ds.get("totalCapacity") +
                                         ", 已用: " + ds.get("usedCapacity") +
                                         ", 活跃: " + ds.get("active") + ")");
                    }
                    if (dataServers.size() > 5) {
                        System.out.println("     ... 还有 " + (dataServers.size() - 5) + " 个DataServer");
                    }
                } else {
                    System.out.println("   DataServer信息为空");
                }
                
                // 4.4 显示主副本分布统计
                System.out.println("4.4 主副本分布统计");
                if (clusterInfo.getReplicaDistribution() != null) {
                    Map<String, Object> replicaDist = clusterInfo.getReplicaDistribution();
                    System.out.println("   总文件数: " + replicaDist.get("totalFiles"));
                    System.out.println("   总目录数: " + replicaDist.get("totalDirectories"));
                    
                    // 显示主副本分布
                    if (replicaDist.get("primaryReplicaCount") instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Integer> primaryCount = (Map<String, Integer>) replicaDist.get("primaryReplicaCount");
                        System.out.println("   各节点主副本分布:");
                        for (Map.Entry<String, Integer> entry : primaryCount.entrySet()) {
                            System.out.println("     " + entry.getKey() + ": " + entry.getValue() + " 个主副本");
                        }
                    }
                    
                    // 显示总副本分布
                    if (replicaDist.get("totalReplicaCount") instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Integer> totalCount = (Map<String, Integer>) replicaDist.get("totalReplicaCount");
                        System.out.println("   各节点总副本分布:");
                        for (Map.Entry<String, Integer> entry : totalCount.entrySet()) {
                            System.out.println("     " + entry.getKey() + ": " + entry.getValue() + " 个副本");
                        }
                    }
                } else {
                    System.out.println("   副本分布信息为空");
                }
                
                // 4.5 显示集群健康状态
                System.out.println("4.5 集群健康状态");
                if (clusterInfo.getHealthStatus() != null) {
                    Map<String, Object> health = clusterInfo.getHealthStatus();
                    System.out.println("   MetaServer健康: " + health.get("metaServerHealthy"));
                    System.out.println("   DataServer健康: " + health.get("dataServerHealthy"));
                    System.out.println("   整体健康状态: " + health.get("overallHealth"));
                } else {
                    System.out.println("   健康状态信息为空");
                }
                
                // 兼容旧版本格式
                if (clusterInfo.getMasterMetaServer() != null) {
                    System.out.println("4.6 旧版本兼容信息");
                    System.out.println("   主MetaServer: " + clusterInfo.getMasterMetaServer());
                    System.out.println("   从MetaServer: " + clusterInfo.getSlaveMetaServer());
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
            try {
                StatInfo nonExistentFile = fileSystem.getFileStats("/non/existent/file.txt");
                System.out.println("   不存在的文件状态: " + (nonExistentFile == null ? "null" : "非null"));
            } catch (Exception e) {
                System.out.println("   访问不存在文件异常: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.2 测试访问不存在的目录
            System.out.println("5.2 测试访问不存在的目录");
            try {
                List<StatInfo> nonExistentDir = fileSystem.listFileStats("/non/existent/directory");
                System.out.println("   不存在的目录列表: " + (nonExistentDir == null ? "null" : "非null"));
            } catch (Exception e) {
                System.out.println("   访问不存在目录异常: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.3 测试删除不存在的文件
            System.out.println("5.3 测试删除不存在的文件");
            try {
                boolean deleteNonExistent = fileSystem.delete("/non/existent/file.txt");
                System.out.println("   删除不存在文件结果: " + deleteNonExistent);
            } catch (Exception e) {
                System.out.println("   删除不存在文件异常: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.4 测试空路径
            System.out.println("5.4 测试空路径");
            try {
                fileSystem.mkdir("");
                System.out.println("   空路径创建目录成功");
            } catch (Exception e) {
                System.out.println("   空路径创建目录失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.5 测试null路径
            System.out.println("5.5 测试null路径");
            try {
                fileSystem.mkdir(null);
                System.out.println("   null路径创建目录成功");
            } catch (Exception e) {
                System.out.println("   null路径创建目录失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.6 测试无效路径格式
            System.out.println("5.6 测试无效路径格式");
            try {
                fileSystem.mkdir("invalid/path");
                System.out.println("   无效路径格式创建目录成功");
            } catch (Exception e) {
                System.out.println("   无效路径格式创建目录失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.7 测试文件路径创建目录
            System.out.println("5.7 测试文件路径创建目录");
            try {
                fileSystem.mkdir("/test_file_path/");
                System.out.println("   文件路径创建目录成功");
            } catch (Exception e) {
                System.out.println("   文件路径创建目录失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.8 测试目录路径创建文件
            System.out.println("5.8 测试目录路径创建文件");
            try {
                fileSystem.create("/test_dir_path/");
                System.out.println("   目录路径创建文件成功");
            } catch (Exception e) {
                System.out.println("   目录路径创建文件失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.9 测试写入空数据
            System.out.println("5.9 测试写入空数据");
            try {
                fileSystem.writeFile("/test_empty_data.txt", new byte[0]);
                System.out.println("   写入空数据成功");
                
                // 验证空文件是否正确创建
                StatInfo emptyFileStats = fileSystem.getFileStats("/test_empty_data.txt");
                if (emptyFileStats != null) {
                    System.out.println("   空文件状态: 大小=" + emptyFileStats.getSize() + "字节");
                }
                
                // 清理测试文件
                fileSystem.delete("/test_empty_data.txt");
            } catch (Exception e) {
                System.out.println("   写入空数据失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            // 5.10 测试写入null数据
            System.out.println("5.10 测试写入null数据");
            try {
                fileSystem.writeFile("/test_null_data.txt", null);
                System.out.println("   写入null数据成功");
            } catch (Exception e) {
                System.out.println("   写入null数据失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
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

            testCase4_ClusterInformation();

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
     * 测试用例7: 高可用演示
     * 场景：先执行一轮基础操作 -> 提示手动停止部分MetaServer/DataServer进程 -> 再执行一轮基础操作以验证系统仍可用
     */
    public void testCase7_HighAvailability() {
        System.out.println("=== 测试用例7: 高可用演示 ===");
        try {
            // 7.0 显示当前集群信息
            System.out.println("7.0 当前集群信息（操作前）");
            try {
                ClusterInfo ci = fileSystem.getClusterInfo();
                if (ci != null) {
                    System.out.println("   Leader: " + (ci.getMetaServers() != null ? ci.getMetaServers().get("leaderAddress") : "unknown"));
                    System.out.println("   Followers: " + (ci.getMetaServers() != null ? ci.getMetaServers().get("followerAddresses") : "[]"));
                    if (ci.getDataServers() != null) {
                        System.out.println("   DataServers: " + ci.getDataServers().size());
                    }
                }
            } catch (Exception e) {
                System.out.println("   获取集群信息失败（可忽略继续）: " + e.getMessage());
            }

            // 7.1 基线操作：创建目录与文件，写入并读取
            System.out.println("7.1 基线操作: 创建/写入/读取");
            fileSystem.mkdir("/test_ha");
            FSOutputStream out1 = fileSystem.create("/test_ha/before_ha.txt");
            String baseContent = "Before HA failover - 基线写入";
            out1.write(baseContent.getBytes("UTF-8"));
            out1.close();
            FSInputStream in1 = fileSystem.open("/test_ha/before_ha.txt");
            byte[] buf1 = new byte[256];
            int n1 = in1.read(buf1);
            in1.close();
            System.out.println("   读取到字节数: " + n1);

            // 7.2 提示手动kill部分节点
            System.out.println("7.2 请在下方倒计时期间手动停止部分节点（示例）：");
            System.out.println("   - 停止一个或多个 MetaServer（除Leader外，或包括Leader以验证自动切换）");
            System.out.println("   - 停止一个或多个 DataServer");
            System.out.println("   Windows 示例（PowerShell 需根据实际PID）：Stop-Process -Id <PID>");
            System.out.println("   Linux/Mac 示例：kill -9 <PID>");
            for (int i = 30; i >= 1; i--) {
                System.out.print("   等待 " + i + " 秒\r");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) { }
            }
            System.out.println();

            // 7.3 再次获取集群信息，观察变化
            System.out.println("7.3 集群信息（节点停止后）");
            try {
                ClusterInfo ci2 = fileSystem.getClusterInfo();
                if (ci2 != null) {
                    System.out.println("   Leader: " + (ci2.getMetaServers() != null ? ci2.getMetaServers().get("leaderAddress") : "unknown"));
                    System.out.println("   Followers: " + (ci2.getMetaServers() != null ? ci2.getMetaServers().get("followerAddresses") : "[]"));
                    if (ci2.getDataServers() != null) {
                        System.out.println("   DataServers: " + ci2.getDataServers().size());
                    }
                }
            } catch (Exception e) {
                System.out.println("   获取集群信息失败（可忽略继续）: " + e.getMessage());
            }

            // 7.4 故障后再次执行基础操作：创建/写入/读取
            System.out.println("7.4 故障后再次执行基础操作");
            FSOutputStream out2 = fileSystem.create("/test_ha/after_ha.txt");
            String afterContent = "After HA failover - 故障后写入";
            out2.write(afterContent.getBytes("UTF-8"));
            out2.close();
            FSInputStream in2 = fileSystem.open("/test_ha/after_ha.txt");
            byte[] buf2 = new byte[256];
            int n2 = in2.read(buf2);
            in2.close();
            System.out.println("   读取到字节数: " + n2);

            // 7.5 清理
            System.out.println("7.5 清理HA演示文件与目录");
            try { fileSystem.delete("/test_ha/before_ha.txt"); } catch (Exception ignore) {}
            try { fileSystem.delete("/test_ha/after_ha.txt"); } catch (Exception ignore) {}
            try { fileSystem.delete("/test_ha"); } catch (Exception ignore) {}
        } catch (Exception e) {
            System.err.println("测试用例7执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("=== 测试用例7完成 ===\n");
    }

    /**
     * 测试用例8: FSCK副本自愈与脏副本清理
     * 步骤：
     * 1) 创建文件并写入
     * 2) 打印当前副本位置
     * 3) 提示手动停止一个DataServer（倒计时），然后触发一次FSCK，验证副本补齐到3
     * 4) 提示手动恢复该DataServer（倒计时），再次触发FSCK，验证历史脏副本被清理（非分配节点不再存在该文件）
     */
    public void testCase8_FsckReplicaSelfHealingAndCleanup() {
        System.out.println("=== 测试用例8: FSCK副本自愈与脏副本清理 ===");
        try {
            String dir = "/test_fsck";
            String path = dir + "/fsck.txt";
            System.out.println("8.1 创建目录与文件");
            fileSystem.mkdir(dir);
            FSOutputStream out = fileSystem.create(path);
            String content = "FSCK HEAL & CLEAN TEST";
            out.write(content.getBytes("UTF-8"));
            out.close();

            System.out.println("8.2 获取文件状态与当前副本");
            StatInfo stat = fileSystem.getFileStats(path);
            if (stat == null || stat.getReplicaData() == null) {
                System.out.println("   获取副本失败，终止用例");
                return;
            }
            java.util.Set<String> currentReplicas = new java.util.HashSet<>();
            for (com.ksyun.campus.client.domain.ReplicaData r : stat.getReplicaData()) {
                currentReplicas.add(r.dsNode);
            }
            System.out.println("   当前副本数: " + currentReplicas.size() + " -> " + currentReplicas);

            System.out.println("8.3 请手动停止一个DataServer（倒计时30秒）...");
            for (int i = 30; i >= 1; i--) {
                System.out.print("   等待 " + i + " 秒\r");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
            }
            System.out.println();

            System.out.println("8.4 触发FSCK自愈（补齐到3副本）");
            String meta = ((EFileSystem)fileSystem).getMetaServerAddress();
            try {
                com.ksyun.campus.client.util.HttpClientUtil.doGet(((EFileSystem)fileSystem).getHttpClient(), "http://" + meta + "/fsck/manual");
                Thread.sleep(2000); // 略等
            } catch (Exception e) {
                System.out.println("   触发FSCK失败: " + e.getMessage());
            }
            StatInfo statAfterHeal = fileSystem.getFileStats(path);
            java.util.Set<String> healedReplicas = new java.util.HashSet<>();
            if (statAfterHeal != null && statAfterHeal.getReplicaData() != null) {
                for (com.ksyun.campus.client.domain.ReplicaData r : statAfterHeal.getReplicaData()) healedReplicas.add(r.dsNode);
            }
            System.out.println("   自愈后副本数: " + healedReplicas.size() + " -> " + healedReplicas);
            if (healedReplicas.size() != 3) {
                System.out.println("   [警告] 自愈后副本数不为3，请检查集群活跃DataServer数量");
            }

            System.out.println("8.5 请手动恢复刚才停止的DataServer（倒计时30秒）...");
            for (int i = 30; i >= 1; i--) {
                System.out.print("   等待 " + i + " 秒\r");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
            }
            System.out.println();

            System.out.println("8.6 再次触发FSCK以清理历史脏副本");
            try {
                com.ksyun.campus.client.util.HttpClientUtil.doGet(((EFileSystem)fileSystem).getHttpClient(), "http://" + meta + "/fsck/manual");
                Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println("   触发FSCK失败: " + e.getMessage());
            }

            System.out.println("8.7 验证：非分配DataServer上不存在该文件（历史脏副本应被清理）");
            ClusterInfo ci = fileSystem.getClusterInfo();
            java.util.List<java.util.Map<String, Object>> dsList = (ci != null && ci.getDataServers() != null) ? ci.getDataServers() : java.util.Collections.emptyList();
            int staleFound = 0;
            for (java.util.Map<String, Object> ds : dsList) {
                Object addr = ds.get("address");
                if (addr == null) continue;
                String a = String.valueOf(addr);
                if (healedReplicas.contains(a)) continue; // 分配内副本，跳过
                try {
                    String url = "http://" + a + "/checkFileExists?path=" + path;
                    String resp = com.ksyun.campus.client.util.HttpClientUtil.doGet(((EFileSystem)fileSystem).getHttpClient(), url);
                    boolean exists = resp != null && resp.toLowerCase().contains("true");
                    if (exists) {
                        staleFound++;
                        System.out.println("   [失败] 在未分配节点仍发现历史脏副本: " + a);
                    }
                } catch (Exception ignore) {}
            }
            if (staleFound == 0) {
                System.out.println("   [成功] 未分配节点未发现历史脏副本");
            }

//            System.out.println("8.8 清理测试数据");
//            try { fileSystem.delete(path); } catch (Exception ignore) {}
//            try { fileSystem.delete(dir); } catch (Exception ignore) {}
        } catch (Exception e) {
            System.err.println("测试用例8执行失败: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("=== 测试用例8完成 ===\n");
    } 

    /**
     * 运行所有测试用例
     */
    public void runAllTestCases() {
        System.out.println("🚀 开始运行所有测试用例...\n");
        
        testCase1_BasicDirectoryOperations();
        testCase2_BasicFileOperations();
        testCase3_FileWriteAndRead();
        //testCase4_ClusterInformation();
        testCase5_ErrorHandling();
        testCase6_PerformanceTest();
        // 高可用演示
        testCase7_HighAvailability();
        // FSCK演示
        testCase8_FsckReplicaSelfHealingAndCleanup();
        
        System.out.println("✅ 所有测试用例执行完成！");
    }
}
