"""
大文件测试示例
演示minFS Python客户端的大文件操作和MD5校验
"""

import os
import tempfile
import hashlib
import logging
from easyClient_python import EFileSystem

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def create_test_file(file_path: str, size: int) -> str:
    """
    创建测试文件
    
    Args:
        file_path: 文件路径
        size: 文件大小（字节）
        
    Returns:
        文件的MD5值
    """
    md5_hash = hashlib.md5()
    
    with open(file_path, 'wb') as f:
        remaining = size
        chunk_size = 1024 * 1024  # 1MB chunks
        
        while remaining > 0:
            # 生成随机数据
            chunk_size = min(chunk_size, remaining)
            chunk = os.urandom(chunk_size)
            f.write(chunk)
            md5_hash.update(chunk)
            remaining -= chunk_size
    
    return md5_hash.hexdigest()

def test_large_file_operations(fs: EFileSystem, file_size: int, remote_path: str):
    """
    测试大文件操作
    
    Args:
        fs: 文件系统客户端
        file_size: 文件大小
        remote_path: 远程文件路径
    """
    print(f"\n=== 测试 {file_size} bytes 文件 ===")
    
    # 创建临时本地文件
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        local_path = temp_file.name
    
    try:
        # 1. 创建测试文件
        print(f"1. 创建 {file_size} bytes 的测试文件...")
        original_md5 = create_test_file(local_path, file_size)
        print(f"   ✓ 本地文件创建成功，MD5: {original_md5}")
        
        # 2. 上传文件到minFS
        print("2. 上传文件到minFS...")
        try:
            with fs.create(remote_path) as f:
                f.write_large_file(local_path)
            print(f"   ✓ 文件上传成功: {remote_path}")
        except Exception as e:
            print(f"   ✗ 文件上传失败: {e}")
            return False
        
        # 3. 验证文件存在
        print("3. 验证文件存在...")
        if fs.exists(remote_path):
            print("   ✓ 文件存在")
        else:
            print("   ✗ 文件不存在")
            return False
        
        # 4. 获取文件信息
        print("4. 获取文件信息...")
        stats = fs.get_file_stats(remote_path)
        if stats and stats.get_size() == file_size:
            print(f"   ✓ 文件大小正确: {stats.get_size()} bytes")
        else:
            print(f"   ✗ 文件大小不正确: 期望 {file_size}, 实际 {stats.get_size() if stats else 0}")
            return False
        
        # 5. 下载文件并验证MD5
        print("5. 下载文件并验证MD5...")
        with tempfile.NamedTemporaryFile(delete=False) as download_file:
            download_path = download_file.name
        
        try:
            with fs.open(remote_path) as f:
                downloaded_md5 = f.calculate_md5()
            
            print(f"   ✓ 下载文件MD5: {downloaded_md5}")
            
            # 验证MD5一致性
            if downloaded_md5 == original_md5:
                print("   ✓ MD5验证成功，文件完整性正确")
            else:
                print(f"   ✗ MD5验证失败")
                print(f"      原始MD5: {original_md5}")
                print(f"      下载MD5: {downloaded_md5}")
                return False
                
        finally:
            # 清理下载的临时文件
            if os.path.exists(download_path):
                os.unlink(download_path)
        
        # 6. 删除远程文件
        print("6. 删除远程文件...")
        if fs.delete(remote_path):
            print("   ✓ 文件删除成功")
        else:
            print("   ✗ 文件删除失败")
            return False
        
        # 7. 验证文件已删除
        print("7. 验证文件已删除...")
        if not fs.exists(remote_path):
            print("   ✓ 文件已成功删除")
        else:
            print("   ✗ 文件仍然存在")
            return False
        
        print("   ✓ 大文件测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ 测试过程中发生错误: {e}")
        return False
    
    finally:
        # 清理本地临时文件
        if os.path.exists(local_path):
            os.unlink(local_path)

def main():
    """主函数"""
    print("=== minFS Python客户端大文件测试示例 ===\n")
    
    # 创建文件系统客户端
    fs = EFileSystem("large_file_test")
    
    try:
        # 测试不同大小的文件
        test_cases = [
            (512 * 1024, "/large_files/512kb.dat"),      # 512KB
            (10 * 1024 * 1024, "/large_files/10mb.dat"), # 10MB
            (100 * 1024 * 1024, "/large_files/100mb.dat") # 100MB
        ]
        
        success_count = 0
        total_count = len(test_cases)
        
        for file_size, remote_path in test_cases:
            if test_large_file_operations(fs, file_size, remote_path):
                success_count += 1
        
        print(f"\n=== 测试结果 ===")
        print(f"成功: {success_count}/{total_count}")
        print(f"失败: {total_count - success_count}/{total_count}")
        
        if success_count == total_count:
            print("🎉 所有大文件测试通过！")
        else:
            print("❌ 部分测试失败")
        
    except Exception as e:
        print(f"\n✗ 执行过程中发生错误: {e}")
    
    finally:
        # 关闭文件系统客户端
        fs.close()

if __name__ == "__main__":
    main()
