"""
文件状态信息类
"""

from typing import List, Optional
from .file_type import FileType
from .replica_data import ReplicaData


class StatInfo:
    """文件状态信息类"""
    
    def __init__(self, path: str = "", size: int = 0, mtime: int = 0, 
                 file_type: FileType = FileType.UNKNOWN, replica_data: Optional[List[ReplicaData]] = None):
        """
        初始化文件状态信息
        
        Args:
            path: 文件路径
            size: 文件大小
            mtime: 修改时间
            file_type: 文件类型
            replica_data: 副本数据列表
        """
        self.path: str = path
        self.size: int = size
        self.mtime: int = mtime
        self.type: FileType = file_type
        self.replica_data: List[ReplicaData] = replica_data or []
    
    def get_path(self) -> str:
        """获取文件路径"""
        return self.path
    
    def set_path(self, path: str):
        """设置文件路径"""
        self.path = path
    
    def get_size(self) -> int:
        """获取文件大小"""
        return self.size
    
    def set_size(self, size: int):
        """设置文件大小"""
        self.size = size
    
    def get_mtime(self) -> int:
        """获取修改时间"""
        return self.mtime
    
    def set_mtime(self, mtime: int):
        """设置修改时间"""
        self.mtime = mtime
    
    def get_type(self) -> FileType:
        """获取文件类型"""
        return self.type
    
    def set_type(self, file_type: FileType):
        """设置文件类型"""
        self.type = file_type
    
    def get_replica_data(self) -> List[ReplicaData]:
        """获取副本数据列表"""
        return self.replica_data
    
    def set_replica_data(self, replica_data: List[ReplicaData]):
        """设置副本数据列表"""
        self.replica_data = replica_data
    
    def is_file(self) -> bool:
        """判断是否为文件"""
        return self.type == FileType.FILE
    
    def is_directory(self) -> bool:
        """判断是否为目录"""
        return self.type == FileType.DIRECTORY
    
    def __str__(self):
        return f"StatInfo{{path='{self.path}', size={self.size}, mtime={self.mtime}, type={self.type}, replica_data={self.replica_data}}}"
    
    def __repr__(self):
        return self.__str__()  
