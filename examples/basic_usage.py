"""
基础使用示例
演示minFS Python客户端的基本用法
"""

import logging
from easyClient_python import EFileSystem

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """主函数"""
    print("=== minFS Python客户端基础使用示例 ===\n")
    
    # 创建文件系统客户端
    fs = EFileSystem("demo_namespace")
    
    try:
        # 1. 创建目录
        print("1. 创建目录...")
        if fs.mkdir("/demo_dir"):
            print("   ✓ 目录创建成功: /demo_dir")
        else:
            print("   ✗ 目录创建失败")
            return
        
        # 2. 创建并写入文件
        print("\n2. 创建并写入文件...")
        try:
            with fs.create("/demo_dir/test.txt") as f:
                f.write_string("Hello, minFS! 这是一个测试文件。")
            print("   ✓ 文件创建并写入成功: /demo_dir/test.txt")
        except Exception as e:
            print(f"   ✗ 文件创建失败: {e}")
            return
        
        # 3. 读取文件
        print("\n3. 读取文件...")
        try:
            with fs.open("/demo_dir/test.txt") as f:
                content = f.read()
                print(f"   ✓ 文件读取成功，内容: {content.decode('utf-8')}")
        except Exception as e:
            print(f"   ✗ 文件读取失败: {e}")
            return
        
        # 4. 获取文件信息
        print("\n4. 获取文件信息...")
        stats = fs.get_file_stats("/demo_dir/test.txt")
        if stats:
            print(f"   ✓ 文件路径: {stats.get_path()}")
            print(f"   ✓ 文件大小: {stats.get_size()} bytes")
            print(f"   ✓ 文件类型: {stats.get_type()}")
            print(f"   ✓ 副本数量: {len(stats.get_replica_data())}")
        else:
            print("   ✗ 获取文件信息失败")
        
        # 5. 列出目录内容
        print("\n5. 列出目录内容...")
        items = fs.list_file_stats("/demo_dir")
        if items:
            print(f"   ✓ 目录中有 {len(items)} 个文件/目录:")
            for item in items:
                print(f"     - {item.get_path()} ({item.get_type()}, {item.get_size()} bytes)")
        else:
            print("   ✗ 列出目录内容失败")
        
        # 6. 检查文件存在性
        print("\n6. 检查文件存在性...")
        if fs.exists("/demo_dir/test.txt"):
            print("   ✓ 文件存在")
        else:
            print("   ✗ 文件不存在")
        
        if fs.is_file("/demo_dir/test.txt"):
            print("   ✓ 是文件")
        else:
            print("   ✗ 不是文件")
        
        if fs.is_directory("/demo_dir"):
            print("   ✓ 是目录")
        else:
            print("   ✗ 不是目录")
        
        # 7. 获取集群信息
        print("\n7. 获取集群信息...")
        cluster_info = fs.get_cluster_info()
        if cluster_info:
            master = cluster_info.get_master_meta_server()
            slave = cluster_info.get_slave_meta_server()
            data_servers = cluster_info.get_data_servers()
            
            if master:
                print(f"   ✓ 主MetaServer: {master.get_host()}:{master.get_port()}")
            if slave:
                print(f"   ✓ 从MetaServer: {slave.get_host()}:{slave.get_port()}")
            print(f"   ✓ DataServer数量: {len(data_servers)}")
            
            if data_servers:
                total_capacity = cluster_info.get_total_capacity()
                used_capacity = cluster_info.get_total_used_capacity()
                usage_percentage = cluster_info.get_usage_percentage()
                print(f"   ✓ 总容量: {total_capacity} bytes")
                print(f"   ✓ 已用容量: {used_capacity} bytes")
                print(f"   ✓ 使用率: {usage_percentage:.2f}%")
        else:
            print("   ✗ 获取集群信息失败")
        
        # 8. 删除文件
        print("\n8. 删除文件...")
        if fs.delete("/demo_dir/test.txt"):
            print("   ✓ 文件删除成功")
        else:
            print("   ✗ 文件删除失败")
        
        # 9. 验证文件已删除
        print("\n9. 验证文件已删除...")
        if not fs.exists("/demo_dir/test.txt"):
            print("   ✓ 文件已成功删除")
        else:
            print("   ✗ 文件仍然存在")
        
        print("\n=== 示例执行完成 ===")
        
    except Exception as e:
        print(f"\n✗ 执行过程中发生错误: {e}")
    
    finally:
        # 关闭文件系统客户端
        fs.close()


if __name__ == "__main__":
    main()
