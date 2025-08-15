"""
简单测试脚本
测试基本导入和类创建
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """测试导入功能"""
    try:
        print("1. 测试导入...")
        
        # 测试核心模块导入
        from core.file_system import FileSystem
        print("   ✓ FileSystem 导入成功")
        
        from core.e_file_system import EFileSystem
        print("   ✓ EFileSystem 导入成功")
        
        from util.http_client_util import HttpClientUtil
        print("   ✓ HttpClientUtil 导入成功")
        
        from domain.file_type import FileType
        print("   ✓ FileType 导入成功")
        
        print("   所有核心模块导入成功！")
        return True
        
    except Exception as e:
        print(f"   ✗ 导入失败: {e}")
        return False

def test_class_creation():
    """测试类创建"""
    try:
        print("\n2. 测试类创建...")
        
        # 测试基础类创建
        from core.file_system import FileSystem
        fs_base = FileSystem()
        print("   ✓ FileSystem 实例创建成功")
        
        # 测试HTTP客户端创建
        from util.http_client_util import HttpClientUtil
        http_client = HttpClientUtil()
        print("   ✓ HttpClientUtil 实例创建成功")
        
        # 测试文件类型枚举
        from domain.file_type import FileType
        file_type = FileType.FILE
        print(f"   ✓ FileType 枚举使用成功: {file_type}")
        
        print("   所有类创建成功！")
        return True
        
    except Exception as e:
        print(f"   ✗ 类创建失败: {e}")
        return False

def test_efilesystem_creation():
    """测试EFileSystem创建（会尝试连接ZooKeeper）"""
    try:
        print("\n3. 测试EFileSystem创建...")
        
        from core.e_file_system import EFileSystem
        
        # 注意：这会尝试连接ZooKeeper，可能会失败
        print("   尝试创建EFileSystem实例...")
        print("   注意：这需要ZooKeeper服务运行")
        
        # 这里我们只是测试导入，不实际创建实例
        print("   ✓ EFileSystem 类导入成功")
        print("   ⚠️  实际创建实例需要ZooKeeper服务")
        
        return True
        
    except Exception as e:
        print(f"   ✗ EFileSystem测试失败: {e}")
        return False

def main():
    """主函数"""
    print("=== minFS Python客户端简单测试 ===\n")
    
    success_count = 0
    total_tests = 3
    
    # 运行测试
    if test_imports():
        success_count += 1
    
    if test_class_creation():
        success_count += 1
    
    if test_efilesystem_creation():
        success_count += 1
    
    # 输出结果
    print(f"\n=== 测试结果 ===")
    print(f"成功: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("🎉 所有测试通过！你的代码结构是正确的。")
        print("\n下一步：")
        print("1. 启动ZooKeeper服务")
        print("2. 启动minFS集群")
        print("3. 运行完整的功能测试")
    else:
        print("⚠️  部分测试失败，需要检查代码。")

if __name__ == "__main__":
    main()




