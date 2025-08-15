"""
集群信息类
"""

from typing import List, Optional
from .meta_server_msg import MetaServerMsg
from .data_server_msg import DataServerMsg


class ClusterInfo:
    """集群信息类"""
    
    def __init__(self, master_meta_server: Optional[MetaServerMsg] = None,
                 slave_meta_server: Optional[MetaServerMsg] = None,
                 data_servers: Optional[List[DataServerMsg]] = None):
        """
        初始化集群信息
        
        Args:
            master_meta_server: 主MetaServer
            slave_meta_server: 从MetaServer
            data_servers: DataServer列表
        """
        self.master_meta_server: Optional[MetaServerMsg] = master_meta_server
        self.slave_meta_server: Optional[MetaServerMsg] = slave_meta_server
        self.data_servers: List[DataServerMsg] = data_servers or []
    
    def get_master_meta_server(self) -> Optional[MetaServerMsg]:
        """获取主MetaServer"""
        return self.master_meta_server
    
    def set_master_meta_server(self, master_meta_server: MetaServerMsg):
        """设置主MetaServer"""
        self.master_meta_server = master_meta_server
    
    def get_slave_meta_server(self) -> Optional[MetaServerMsg]:
        """获取从MetaServer"""
        return self.slave_meta_server
    
    def set_slave_meta_server(self, slave_meta_server: MetaServerMsg):
        """设置从MetaServer"""
        self.slave_meta_server = slave_meta_server
    
    def get_data_servers(self) -> List[DataServerMsg]:
        """获取DataServer列表"""
        return self.data_servers
    
    def set_data_servers(self, data_servers: List[DataServerMsg]):
        """设置DataServer列表"""
        self.data_servers = data_servers
    
    def add_data_server(self, data_server: DataServerMsg):
        """添加DataServer"""
        self.data_servers.append(data_server)
    
    def get_available_data_servers(self) -> List[DataServerMsg]:
        """获取可用的DataServer列表"""
        return [ds for ds in self.data_servers if ds.get_available_capacity() > 0]
    
    def get_total_capacity(self) -> int:
        """获取总容量"""
        return sum(ds.get_capacity() for ds in self.data_servers)
    
    def get_total_used_capacity(self) -> int:
        """获取总已用容量"""
        return sum(ds.get_use_capacity() for ds in self.data_servers)
    
    def get_total_available_capacity(self) -> int:
        """获取总可用容量"""
        return sum(ds.get_available_capacity() for ds in self.data_servers)
    
    def get_usage_percentage(self) -> float:
        """获取总体使用百分比"""
        total_capacity = self.get_total_capacity()
        if total_capacity == 0:
            return 0.0
        return (self.get_total_used_capacity() / total_capacity) * 100
    
    def __str__(self):
        return f"ClusterInfo{{master_meta_server={self.master_meta_server}, slave_meta_server={self.slave_meta_server}, data_servers={self.data_servers}}}"
    
    def __repr__(self):
        return self.__str__()
