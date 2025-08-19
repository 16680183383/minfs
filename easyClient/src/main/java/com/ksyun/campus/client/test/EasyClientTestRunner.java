package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.util.ZkUtil;

/**
 * easyClient 测试运行器
 * 提供命令行接口来运行各种测试用例
 * 
 * 使用方法:
 * java -cp easyClient-1.0.jar com.ksyun.campus.client.test.EasyClientTestRunner [测试用例编号]
 * 
 * 测试用例编号:
 * 1 - 基础目录操作
 * 2 - 基础文件操作  
 * 3 - 文件写入和读取
 * 4 - 集群信息获取
 * 5 - 错误处理和边界情况
 * 6 - 性能测试
 * all - 运行所有测试用例
 */
public class EasyClientTestRunner {
    
    public static void main(String[] args) {
        System.out.println("🎯 easyClient SDK 测试运行器");
        System.out.println("版本: 1.0");
        System.out.println("=====================================\n");
        
        try {
            // 初始化文件系统
            System.out.println("正在初始化文件系统...");
            EFileSystem fileSystem = new EFileSystem("test");
            System.out.println("✅ 文件系统初始化成功\n");
            
            // 创建测试用例集合
            FileSystemTestCases testCases = new FileSystemTestCases(fileSystem);
            
            // 解析命令行参数
            if (args.length == 0) {
                // 默认运行所有测试用例
                System.out.println("未指定测试用例，默认运行所有测试用例...\n");
                testCases.runAllTestCases();
            } else {
                String testCase = args[0].toLowerCase();
                
                switch (testCase) {
                    case "1":
                        System.out.println("运行测试用例1: 基础目录操作\n");
                        testCases.testCase1_BasicDirectoryOperations();
                        break;
                        
                    case "2":
                        System.out.println("运行测试用例2: 基础文件操作\n");
                        testCases.testCase2_BasicFileOperations();
                        break;
                        
                    case "3":
                        System.out.println("运行测试用例3: 文件写入和读取\n");
                        testCases.testCase3_FileWriteAndRead();
                        break;
                        
                    case "4":
                        System.out.println("运行测试用例4: 集群信息获取\n");
                        testCases.testCase4_ClusterInformation();
                        break;
                        
                    case "5":
                        System.out.println("运行测试用例5: 错误处理和边界情况\n");
                        testCases.testCase5_ErrorHandling();
                        break;
                        
                    case "6":
                        System.out.println("运行测试用例6: 性能测试\n");
                        testCases.testCase6_PerformanceTest();
                        break;
                    case "7":
                        System.out.println("运行测试用例7: 高可用演示\n");
                        testCases.testCase7_HighAvailability();
                        break;
                        
                    case "all":
                        System.out.println("运行所有测试用例\n");
                        testCases.runAllTestCases();
                        break;
                        
                    default:
                        System.out.println("❌ 无效的测试用例编号: " + testCase);
                        printUsage();
                        break;
                }
            }
            
        } catch (Exception e) {
            System.err.println("❌ 测试运行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("\n🎉 测试运行完成！");
    }
    
    /**
     * 打印使用说明
     */
    private static void printUsage() {
        System.out.println("\n📖 使用方法:");
        System.out.println("java -cp easyClient-1.0.jar com.ksyun.campus.client.test.EasyClientTestRunner [测试用例编号]");
        System.out.println("\n📋 可用的测试用例:");
        System.out.println("  1  - 基础目录操作");
        System.out.println("  2  - 基础文件操作");
        System.out.println("  3  - 文件写入和读取");
        System.out.println("  4  - 集群信息获取");
        System.out.println("  5  - 错误处理和边界情况");
        System.out.println("  6  - 性能测试");
        System.out.println("  7  - 高可用演示");
        System.out.println("  all - 运行所有测试用例");
        System.out.println("\n💡 示例:");
        System.out.println("  java -cp easyClient-1.0.jar com.ksyun.campus.client.test.EasyClientTestRunner 1");
        System.out.println("  java -cp easyClient-1.0.jar com.ksyun.campus.client.test.EasyClientTestRunner all");
    }
}
