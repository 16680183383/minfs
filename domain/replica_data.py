"""
副本数据类
"""

from typing import Optional


class ReplicaData:
    """副本数据类"""
    
    def __init__(self, replica_id: str = "", ds_node: str = "", path: str = ""):
        """
        初始化副本数据
        
        Args:
            replica_id: 副本ID
            ds_node: 数据节点，格式为ip:port
            path: 文件路径
        """
        self.id: str = replica_id
        self.ds_node: str = ds_node  # 格式为ip:port
        self.path: str = path
    
    def get_host(self) -> str:
        """获取主机地址"""
        if ':' in self.ds_node:
            return self.ds_node.split(':')[0]
        return ""
    
    def get_port(self) -> int:
        """获取端口号"""
        if ':' in self.ds_node:
            try:
                return int(self.ds_node.split(':')[1])
            except (ValueError, IndexError):
                return 0
        return 0
    
    def __str__(self):
        return f"ReplicaData{{id='{self.id}', ds_node='{self.ds_node}', path='{self.path}'}}"
    
    def __repr__(self):
        return self.__str__()
