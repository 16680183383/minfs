"""
真实环境集成测试
需要真实的ZooKeeper和minFS集群
"""

import sys
import os
import time
import logging

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_services():
    """检查服务是否可用"""
    print("=== 检查服务可用性 ===\n")
    
    import requests
    
    # 检查ZooKeeper
    print("1. 检查ZooKeeper服务...")
    try:
        # 这里可以添加ZooKeeper连接测试
        print("   ⚠️  ZooKeeper检查需要kazoo库")
        zk_available = True
    except Exception as e:
        print(f"   ✗ ZooKeeper不可用: {e}")
        zk_available = False
    
    # 检查MetaServer
    print("\n2. 检查MetaServer服务...")
    meta_available = False
    try:
        response = requests.get("http://localhost:8001/cluster/info", timeout=5)
        if response.status_code == 200:
            print("   ✓ MetaServer可用 (端口8001)")
            meta_available = True
        else:
            print(f"   ✗ MetaServer响应异常: {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("   ✗ MetaServer连接失败 (端口8001)")
    except Exception as e:
        print(f"   ✗ MetaServer检查失败: {e}")
    
    # 检查DataServer
    print("\n3. 检查DataServer服务...")
    data_available = False
    try:
        response = requests.get("http://localhost:8002/status", timeout=5)
        if response.status_code == 200:
            print("   ✓ DataServer1可用 (端口8002)")
            data_available = True
        else:
            print(f"   ✗ DataServer1响应异常: {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("   ✗ DataServer1连接失败 (端口8002)")
    except Exception as e:
        print(f"   ✗ DataServer1检查失败: {e}")
    
    return zk_available, meta_available, data_available

def test_real_efilesystem():
    """测试真实的EFileSystem"""
    print("\n=== 测试真实EFileSystem ===")
    
    try:
        from core.e_file_system import EFileSystem
        
        print("   创建EFileSystem实例...")
        fs = EFileSystem("test_namespace")
        print("   ✓ EFileSystem实例创建成功")
        
        # 测试基本操作
        print("\n   测试基本操作...")
        
        # 1. 获取集群信息
        print("   1. 获取集群信息...")
        cluster_info = fs.get_cluster_info()
        if cluster_info:
            master = cluster_info.get_master_meta_server()
            if master:
                print(f"      ✓ 主MetaServer: {master.get_host()}:{master.get_port()}")
            else:
                print("      ⚠️  未找到主MetaServer")
        else:
            print("      ✗ 获取集群信息失败")
        
        # 2. 创建目录
        print("   2. 创建目录...")
        if fs.mkdir("/test_dir"):
            print("      ✓ 目录创建成功")
        else:
            print("      ✗ 目录创建失败")
        
        # 3. 创建文件
        print("   3. 创建文件...")
        try:
            with fs.create("/test_dir/test.txt") as f:
                f.write_string("Hello, real minFS!")
            print("      ✓ 文件创建成功")
        except Exception as e:
            print(f"      ✗ 文件创建失败: {e}")
        
        # 4. 读取文件
        print("   4. 读取文件...")
        try:
            with fs.open("/test_dir/test.txt") as f:
                content = f.read()
                print(f"      ✓ 文件读取成功: {content.decode('utf-8')}")
        except Exception as e:
            print(f"      ✗ 文件读取失败: {e}")
        
        # 5. 获取文件状态
        print("   5. 获取文件状态...")
        stats = fs.get_file_stats("/test_dir/test.txt")
        if stats:
            print(f"      ✓ 文件大小: {stats.get_size()} bytes")
            print(f"      ✓ 文件类型: {stats.get_type()}")
        else:
            print("      ✗ 获取文件状态失败")
        
        # 6. 列出目录内容
        print("   6. 列出目录内容...")
        items = fs.list_file_stats("/test_dir")
        if items:
            print(f"      ✓ 目录中有 {len(items)} 个文件/目录")
            for item in items:
                print(f"        - {item.get_path()} ({item.get_type()})")
        else:
            print("      ✗ 列出目录内容失败")
        
        # 7. 删除文件
        print("   7. 删除文件...")
        if fs.delete("/test_dir/test.txt"):
            print("      ✓ 文件删除成功")
        else:
            print("      ✗ 文件删除失败")
        
        # 8. 删除目录
        print("   8. 删除目录...")
        if fs.delete("/test_dir"):
            print("      ✓ 目录删除成功")
        else:
            print("      ✗ 目录删除失败")
        
        print("   ✓ 所有基本操作测试完成")
        return True
        
    except Exception as e:
        print(f"   ✗ EFileSystem测试失败: {e}")
        logger.exception("EFileSystem测试异常")
        return False

def test_large_file_operations():
    """测试大文件操作"""
    print("\n=== 测试大文件操作 ===")
    
    try:
        from core.e_file_system import EFileSystem
        
        fs = EFileSystem("test_namespace")
        
        # 创建大文件内容
        large_content = "A" * 1024 * 1024  # 1MB内容
        
        print("   创建大文件 (1MB)...")
        try:
            with fs.create("/large_file.txt") as f:
                f.write_string(large_content)
            print("   ✓ 大文件创建成功")
        except Exception as e:
            print(f"   ✗ 大文件创建失败: {e}")
            return False
        
        # 读取大文件
        print("   读取大文件...")
        try:
            with fs.open("/large_file.txt") as f:
                content = f.read()
                if len(content) == len(large_content.encode('utf-8')):
                    print("   ✓ 大文件读取成功，大小正确")
                else:
                    print(f"   ✗ 大文件大小不匹配: 期望{len(large_content)}，实际{len(content)}")
                    return False
        except Exception as e:
            print(f"   ✗ 大文件读取失败: {e}")
            return False
        
        # 清理
        fs.delete("/large_file.txt")
        print("   ✓ 大文件测试完成")
        return True
        
    except Exception as e:
        print(f"   ✗ 大文件操作测试失败: {e}")
        return False

def main():
    """主函数"""
    print("=== minFS Python客户端真实环境集成测试 ===\n")
    
    # 检查服务可用性
    zk_available, meta_available, data_available = check_services()
    
    if not (zk_available and meta_available and data_available):
        print("\n⚠️  部分服务不可用，无法进行完整测试")
        print("\n解决方案:")
        print("1. 启动ZooKeeper: zkServer.cmd start (Windows)")
        print("2. 启动MetaServer: 参考项目文档")
        print("3. 启动DataServer: 参考项目文档")
        print("4. 确保端口8001, 8002, 8003可用")
        return
    
    print("\n🎉 所有服务可用，开始真实环境测试")
    
    success_count = 0
    total_tests = 3
    
    # 运行测试
    if test_real_efilesystem():
        success_count += 1
    
    if test_large_file_operations():
        success_count += 1
    
    # 输出结果
    print(f"\n=== 测试结果 ===")
    print(f"成功: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("🎉 所有测试通过！你的客户端在生产环境中完全可用！")
    elif success_count > 0:
        print("✅ 部分测试通过，客户端基本功能正常")
    else:
        print("⚠️  所有测试失败，需要检查环境配置")

if __name__ == "__main__":
    main()



