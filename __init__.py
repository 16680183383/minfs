"""
minFS分布式文件系统 - Python客户端SDK
提供分布式文件系统的基础操作接口
"""

__version__ = "1.0.0"
__author__ = "minFS Team"

from .core.e_file_system import EFileSystem
from .domain.stat_info import StatInfo
from .domain.cluster_info import ClusterInfo
from .domain.file_type import FileType

__all__ = [
    'EFileSystem',
    'StatInfo', 
    'ClusterInfo',
    'FileType'
]
