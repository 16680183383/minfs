"""
文件输出流类
用于向分布式文件系统写入文件数据
"""

import hashlib
import logging
from typing import Optional, List
from domain.replica_data import ReplicaData
from util.http_client_util import HttpClientUtil

logger = logging.getLogger(__name__)


class FSOutputStream:
    """文件输出流类"""
    
    def __init__(self, path: str, replica_data: List[ReplicaData], http_client: Optional[HttpClientUtil] = None):
        """
        初始化文件输出流
        
        Args:     
            path: 文件路径
            replica_data: 副本数据列表
            http_client: HTTP客户端
        """
        self.path = path
        self.replica_data = replica_data
        self.http_client = http_client or HttpClientUtil()
        self.current_position = 0
        self.md5_hash = hashlib.md5()
        self._buffer = b""
        self._buffer_size = 1024 * 1024  # 1MB buffer
        self._closed = False
    
    def write(self, data: bytes):
        """
        写入数据
        
        Args:
            data: 要写入的数据
        """
        if self._closed:
            raise ValueError("Stream is closed")
        
        if not data:
            return
        
        # 更新MD5哈希
        self.md5_hash.update(data)
        
        # 添加到缓冲区
        self._buffer += data
        
        # 如果缓冲区满了，刷新到服务器
        if len(self._buffer) >= self._buffer_size:
            self.flush()
    
    def write_string(self, text: str, encoding: str = 'utf-8'):
        """
        写入字符串
        
        Args:
            text: 要写入的字符串
            encoding: 编码格式
        """
        data = text.encode(encoding)
        self.write(data)
    
    def flush(self):
        """刷新缓冲区"""
        if self._buffer and not self._closed:
            self._write_buffer()
    
    def _write_buffer(self):
        """将缓冲区数据写入服务器"""
        if not self._buffer:
            return
        
        # 向所有副本写入数据
        success_count = 0
        for replica in self.replica_data:
            try:
                url = f"http://{replica.ds_node}/file/write"
                params = {
                    "path": replica.path,
                    "offset": self.current_position
                }
                
                response = self.http_client.post(url, params=params, data=self._buffer)
                if response.status_code == 200:
                    success_count += 1
                    logger.debug(f"Successfully wrote {len(self._buffer)} bytes to {replica.ds_node}")
                else:
                    logger.warning(f"Failed to write to {replica.ds_node}: {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Failed to write to {replica.ds_node}: {e}")
                continue
        
        if success_count > 0:
            self.current_position += len(self._buffer)
            self._buffer = b""
            logger.info(f"Successfully wrote data to {success_count}/{len(self.replica_data)} replicas")
        else:
            raise IOError(f"Failed to write data to any replica for {self.path}")
    
    def seek(self, offset: int, whence: int = 0):
        """
        设置写入位置
        
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
            # 需要先获取文件大小
            new_position = self._get_file_size() + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")
        
        if new_position < 0:
            new_position = 0
        
        # 刷新当前缓冲区
        self.flush()
        
        self.current_position = new_position
    
    def tell(self) -> int:
        """获取当前写入位置"""
        return self.current_position
    
    def _get_file_size(self) -> int:
        """获取文件大小"""
        if not self.replica_data:
            return 0
        
        try:
            replica = self.replica_data[0]
            url = f"http://{replica.ds_node}/file/size"
            params = {"path": replica.path}
            
            response = self.http_client.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get("size", 0)
        except Exception as e:
            logger.error(f"Failed to get file size: {e}")
        
        return 0
    
    def close(self):
        """关闭流"""
        if not self._closed:
            self.flush()
            self._closed = True
            logger.info(f"Closed output stream for {self.path}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_md5(self) -> str:
        """
        获取当前数据的MD5值
        
        Returns:
            MD5哈希值
        """
        return self.md5_hash.hexdigest()
    
    def write_file(self, file_path: str, chunk_size: int = 8192):
        """
        写入本地文件
        
        Args:
            file_path: 本地文件路径
            chunk_size: 分块大小
        """
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    self.write(chunk)
        except Exception as e:
            logger.error(f"Failed to write file {file_path}: {e}")
            raise
    
    def write_large_file(self, file_path: str, chunk_size: int = 1024 * 1024):
        """
        写入大文件（带进度显示）
        
        Args:
            file_path: 本地文件路径
            chunk_size: 分块大小
        """
        import os
        
        file_size = os.path.getsize(file_path)
        written = 0
        
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    self.write(chunk)
                    written += len(chunk)
                    
                    # 显示进度
                    progress = (written / file_size) * 100
                    logger.info(f"Progress: {progress:.1f}% ({written}/{file_size} bytes)")
                    
        except Exception as e:
            logger.error(f"Failed to write large file {file_path}: {e}")
            raise
