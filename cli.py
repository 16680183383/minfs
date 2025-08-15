"""
minFS Python客户端命令行接口
"""

import argparse
import logging
import sys
from easyClient_python import EFileSystem

def setup_logging(verbose: bool = False):
    """设置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def ls_command(fs: EFileSystem, path: str):
    """列出目录内容"""
    print(f"列出目录: {path}")
    items = fs.list_file_stats(path)
    
    if not items:
        print("  目录为空")
        return
    
    print(f"  找到 {len(items)} 个项目:")
    for item in items:
        size_str = f"{item.get_size()} bytes" if item.get_size() > 0 else "目录"
        print(f"    {item.get_path()} ({item.get_type()}, {size_str})")

def stat_command(fs: EFileSystem, path: str):
    """显示文件状态"""
    print(f"文件状态: {path}")
    stats = fs.get_file_stats(path)
    
    if not stats:
        print("  文件不存在")
        return
    
    print(f"  路径: {stats.get_path()}")
    print(f"  大小: {stats.get_size()} bytes")
    print(f"  类型: {stats.get_type()}")
    print(f"  副本数: {len(stats.get_replica_data())}")
    
    for i, replica in enumerate(stats.get_replica_data(), 1):
        print(f"    副本 {i}: {replica.ds_node} -> {replica.path}")

def cat_command(fs: EFileSystem, path: str):
    """显示文件内容"""
    print(f"文件内容: {path}")
    try:
        with fs.open(path) as f:
            content = f.read()
            print(content.decode('utf-8', errors='ignore'))
    except Exception as e:
        print(f"  读取失败: {e}")

def mkdir_command(fs: EFileSystem, path: str):
    """创建目录"""
    print(f"创建目录: {path}")
    if fs.mkdir(path):
        print("  ✓ 创建成功")
    else:
        print("  ✗ 创建失败")

def rm_command(fs: EFileSystem, path: str):
    """删除文件或目录"""
    print(f"删除: {path}")
    if fs.delete(path):
        print("  ✓ 删除成功")
    else:
        print("  ✗ 删除失败")

def cluster_command(fs: EFileSystem):
    """显示集群信息"""
    print("集群信息:")
    cluster_info = fs.get_cluster_info()
    
    master = cluster_info.get_master_meta_server()
    slave = cluster_info.get_slave_meta_server()
    data_servers = cluster_info.get_data_servers()
    
    if master:
        print(f"  主MetaServer: {master.get_host()}:{master.get_port()}")
    if slave:
        print(f"  从MetaServer: {slave.get_host()}:{slave.get_port()}")
    
    print(f"  DataServer数量: {len(data_servers)}")
    
    if data_servers:
        total_capacity = cluster_info.get_total_capacity()
        used_capacity = cluster_info.get_total_used_capacity()
        usage_percentage = cluster_info.get_usage_percentage()
        print(f"  总容量: {total_capacity} bytes")
        print(f"  已用容量: {used_capacity} bytes")
        print(f"  使用率: {usage_percentage:.2f}%")
        
        print("  DataServer列表:")
        for i, ds in enumerate(data_servers, 1):
            print(f"    {i}. {ds.get_host()}:{ds.get_port()} "
                  f"(容量: {ds.get_capacity()}, 已用: {ds.get_use_capacity()})")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="minFS Python客户端命令行工具")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    parser.add_argument("--namespace", "-n", default="default", help="文件系统命名空间")
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # ls命令
    ls_parser = subparsers.add_parser("ls", help="列出目录内容")
    ls_parser.add_argument("path", nargs="?", default="/", help="目录路径")
    
    # stat命令
    stat_parser = subparsers.add_parser("stat", help="显示文件状态")
    stat_parser.add_argument("path", help="文件路径")
    
    # cat命令
    cat_parser = subparsers.add_parser("cat", help="显示文件内容")
    cat_parser.add_argument("path", help="文件路径")
    
    # mkdir命令
    mkdir_parser = subparsers.add_parser("mkdir", help="创建目录")
    mkdir_parser.add_argument("path", help="目录路径")
    
    # rm命令
    rm_parser = subparsers.add_parser("rm", help="删除文件或目录")
    rm_parser.add_argument("path", help="文件或目录路径")
    
    # cluster命令
    subparsers.add_parser("cluster", help="显示集群信息")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 设置日志
    setup_logging(args.verbose)
    
    # 创建文件系统客户端
    try:
        fs = EFileSystem(args.namespace)
    except Exception as e:
        print(f"连接失败: {e}")
        sys.exit(1)
    
    try:
        # 执行命令
        if args.command == "ls":
            ls_command(fs, args.path)
        elif args.command == "stat":
            stat_command(fs, args.path)
        elif args.command == "cat":
            cat_command(fs, args.path)
        elif args.command == "mkdir":
            mkdir_command(fs, args.path)
        elif args.command == "rm":
            rm_command(fs, args.path)
        elif args.command == "cluster":
            cluster_command(fs)
    
    except Exception as e:
        print(f"命令执行失败: {e}")
        sys.exit(1)
    
    finally:
        fs.close()

if __name__ == "__main__":
    main()
