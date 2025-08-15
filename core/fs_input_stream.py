"""
文件输入流类
用于从分布式文件系统读取文件数据
"""

import hashlib
import logging
from typing import Optional, List
from domain.replica_data import ReplicaData
from util.http_client_util import HttpClientUtil

logger = logging.getLogger(__name__)


class FSInputStream:
    """文件输入流类"""
    
    def __init__(self, path: str, replica_data: List[ReplicaData], http_client: Optional[HttpClientUtil] = None):
        """
        初始化文件输入流
        
        Args:
            path: 文件路径
            replica_data: 副本数据列表
            http_client: HTTP客户端
        """
        self.path = path
        self.replica_data = replica_data
        self.http_client = http_client or HttpClientUtil()
        self.current_position = 0
        self.file_size = 0
        self._buffer = b""
        self._buffer_position = 0
        
        # 获取文件大小
        if replica_data:
            self._get_file_size()
    
    def _get_file_size(self):
        """获取文件大小"""
        try:
            # 从第一个副本获取文件大小
            replica = self.replica_data[0]
            url = f"http://{replica.ds_node}/file/size"
            params = {"path": replica.path}
            
            response = self.http_client.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                self.file_size = data.get("size", 0)
                logger.info(f"File size for {self.path}: {self.file_size}")
        except Exception as e:
            logger.error(f"Failed to get file size for {self.path}: {e}")
    
    def read(self, size: int = -1) -> bytes:
        """
        读取文件数据
        
        Args:
            size: 读取大小，-1表示读取全部
            
        Returns:
            读取的数据
        """
        if size == 0:
            return b""
        
        if size == -1:
            # 读取全部数据
            return self._read_all()
        
        # 读取指定大小的数据
        result = b""
        remaining = size
        
        while remaining > 0:
            # 从缓冲区读取
            if self._buffer and self._buffer_position < len(self._buffer):
                available = len(self._buffer) - self._buffer_position
                read_size = min(available, remaining)
                result += self._buffer[self._buffer_position:self._buffer_position + read_size]
                self._buffer_position += read_size
                self.current_position += read_size
                remaining -= read_size
            else:
                # 从服务器读取更多数据
                chunk = self._read_chunk(min(remaining, 1024 * 1024))  # 1MB chunks
                if not chunk:
                    break
                result += chunk
                remaining -= len(chunk)
        
        return result
    
    def _read_all(self) -> bytes:
        """读取全部数据"""
        result = b""
        
        # 先读取缓冲区中的数据
        if self._buffer and self._buffer_position < len(self._buffer):
            result += self._buffer[self._buffer_position:]
            self.current_position += len(self._buffer) - self._buffer_position
            self._buffer_position = len(self._buffer)
        
        # 从服务器读取剩余数据
        while self.current_position < self.file_size:
            chunk = self._read_chunk(1024 * 1024)  # 1MB chunks
            if not chunk:
                break
            result += chunk
        
        return result
    
    def _read_chunk(self, size: int) -> bytes:
        """
        从服务器读取数据块
        
        Args:
            size: 读取大小
            
        Returns:
            读取的数据块
        """
        if not self.replica_data:
            return b""
        
        # 尝试从所有副本读取，直到成功
        for replica in self.replica_data:
            try:
                url = f"http://{replica.ds_node}/file/read"
                params = {
                    "path": replica.path,
                    "offset": self.current_position,
                    "size": size
                }
                
                response = self.http_client.get(url, params=params)
                if response.status_code == 200:
                    data = response.content
                    self._buffer = data
                    self._buffer_position = 0
                    return data
                    
            except Exception as e:
                logger.warning(f"Failed to read from replica {replica.ds_node}: {e}")
                continue
        
        logger.error(f"Failed to read from all replicas for {self.path}")
        return b""
    
    def seek(self, offset: int, whence: int = 0):
        """
        设置读取位置
        
        Args:
            offset: 偏移量
            whence: 参考位置 (0: 文件开头, 1: 当前位置, 2: 文件结尾)
        """
        if whence == 0:
            # 从文件开头
            new_position = offset
        elif whence == 1:
            # 从当前位置
            new_position = self.current_position + offset
        elif whence == 2:
            # 从文件结尾
            new_position = self.file_size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")
        
        if new_position < 0:
            new_position = 0
        elif new_position > self.file_size:
            new_position = self.file_size
        
        self.current_position = new_position
        self._buffer = b""
        self._buffer_position = 0
    
    def tell(self) -> int:
        """获取当前读取位置"""
        return self.current_position
    
    def close(self):
        """关闭流"""
        self._buffer = b""
        self._buffer_position = 0
        self.current_position = 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def calculate_md5(self) -> str:
        """
        计算文件的MD5值
        
        Returns:
            MD5哈希值
        """
        md5_hash = hashlib.md5()
        original_position = self.current_position
        
        try:
            self.seek(0)  # 回到文件开头
            while True:
                chunk = self.read(8192)  # 8KB chunks
                if not chunk:
                    break
                md5_hash.update(chunk)
        finally:
            self.seek(original_position)  # 恢复原位置
        
        return md5_hash.hexdigest()
    
    def __iter__(self):
        """迭代器支持"""
        return self
    
    def __next__(self):
        """迭代器下一项"""
        chunk = self.read(8192)  # 8KB chunks
        if not chunk:
            raise StopIteration
        return chunk
