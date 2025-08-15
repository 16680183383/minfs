"""
文件类型枚举
"""

from enum import Enum


class FileType(Enum):
    """文件类型枚举"""
    UNKNOWN = 0
    VOLUME = 1
    FILE = 2
    DIRECTORY = 3
    
    @classmethod
    def get(cls, code: int) -> 'FileType':
        """
        根据代码获取文件类型
        
        Args:
            code: 文件类型代码
            
        Returns:
            对应的文件类型枚举值
        """
        for file_type in cls:
            if file_type.value == code:
                return file_type
        return cls.UNKNOWN
    
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return f"FileType.{self.name}"
