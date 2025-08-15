"""
分布式文件系统客户端主类
提供文件操作的基础接口
"""

import logging
import json
from typing import List, Optional
from .file_system import FileSystem
from .fs_input_stream import FSInputStream
from .fs_output_stream import FSOutputStream
from domain.stat_info import StatInfo
from domain.cluster_info import ClusterInfo
from domain.file_type import FileType
from domain.replica_data import ReplicaData
from util.zk_util import ZkUtil

logger = logging.getLogger(__name__)


class EFileSystem(FileSystem):
    """分布式文件系统客户端主类"""
    
    def __init__(self, file_system_name: str = "default"):
        """
        初始化文件系统客户端
        
        Args:
            file_system_name: 文件系统名称（命名空间）
        """    
        super().__init__()
        self.default_file_system_name = file_system_name
        self.zk_util = ZkUtil()
        
        # 连接ZooKeeper
        try:
            self.zk_util.connect()
            logger.info(f"Initialized EFileSystem with namespace: {file_system_name}")
        except Exception as e:
            logger.warning(f"Failed to connect to ZooKeeper: {e}")
    
    def open(self, path: str) -> FSInputStream:
        """
        打开文件用于读取
        
        Args:
            path: 文件路径
            
        Returns:
            文件输入流
        """
        try:
            # 获取文件元数据
            stat_info = self.get_file_stats(path)
            if not stat_info or not stat_info.is_file():
                raise FileNotFoundError(f"File not found or not a file: {path}")
            
            # 创建输入流
            input_stream = FSInputStream(path, stat_info.get_replica_data(), self.http_client)
            logger.info(f"Opened file for reading: {path}")
            return input_stream
            
        except Exception as e:
            logger.error(f"Failed to open file {path}: {e}")
            raise
    
    def create(self, path: str) -> FSOutputStream:
        """
        创建文件用于写入
        
        Args:
            path: 文件路径
            
        Returns:
            文件输出流
        """
        try:
            # 调用MetaServer创建文件
            meta_server = self.zk_util.get_master_meta_server()
            if not meta_server:
                raise RuntimeError("No available MetaServer")
            
            url = f"{meta_server.get_url()} /file/create"
            data = {
                "path": path,
                "fileSystemName"     : self.default_file_system_name
            }
            
            response = self._call_remote("POST", url, json_data=data)
            if response.status_code != 200:
                raise RuntimeError(f"Failed to create file: {response.text}")
            
            # 解析响应获取副本信息
            result = response.json()
            replica_data = []
            for replica_info in result.get("replicaData", []):
                replica = ReplicaData(
                    replica_id=replica_info.get("id", ""),
                    ds_node=replica_info.get("dsNode", ""),
                    path=replica_info.get("path", "")
                )
                replica_data.append(replica)
            
            # 创建输出流
            output_stream = FSOutputStream(path, replica_data, self.http_client)
            logger.info(f"Created file for writing: {path}")
            return output_stream
            
        except Exception as e:
            logger.error(f"Failed to create file {path}: {e}")
            raise
    
    def mkdir(self, path: str) -> bool:
        """
        创建目录
        
        Args:
            path: 目录路径
            
        Returns:
            是否创建成功
        """
        try:
            meta_server = self.zk_util.get_master_meta_server()
            if not meta_server:
                raise RuntimeError("No available MetaServer")
            
            url = f"{meta_server.get_url()}/directory/create"
            data = {
                "path": path,
                "fileSystemName": self.default_file_system_name
            }
            
            response = self._call_remote("POST", url, json_data=data)
            success = response.status_code == 200
            
            if success:
                logger.info(f"Created directory: {path}")
            else:
                logger.error(f"Failed to create directory {path}: {response.text}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {e}")
            return False
    
    def delete(self, path: str) -> bool:
        """
        删除文件或目录（支持递归删除）
        
        Args:
            path: 文件或目录路径
            
        Returns:
            是否删除成功
        """
        try:
            meta_server = self.zk_util.get_master_meta_server()
            if not meta_server:
                raise RuntimeError("No available MetaServer")
            
            url = f"{meta_server.get_url()}/file/delete"
            data = {
                "path": path,
                "fileSystemName": self.default_file_system_name,
                "recursive": True
            }
            
            response = self._call_remote("DELETE", url, json_data=data)
            success = response.status_code == 200
            
            if success:
                logger.info(f"Deleted: {path}")
            else:
                logger.error(f"Failed to delete {path}: {response.text}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to delete {path}: {e}")
            return False
    
    def get_file_stats(self, path: str) -> Optional[StatInfo]:
        """
        获取文件状态信息
        
        Args:
            path: 文件路径
            
        Returns:
            文件状态信息
        """
        try:
            meta_server = self.zk_util.get_master_meta_server()
            if not meta_server:
                raise RuntimeError("No available MetaServer")
            
            url = f"{meta_server.get_url()}/file/stats"
            params = {
                "path": path,
                "fileSystemName": self.default_file_system_name
            }
            
            response = self._call_remote("GET", url, params=params)
            if response.status_code != 200:
                return None
            
            data = response.json()
            
            # 解析副本数据
            replica_data = []
            for replica_info in data.get("replicaData", []):
                replica = ReplicaData(
                    replica_id=replica_info.get("id", ""),
                    ds_node=replica_info.get("dsNode", ""),
                    path=replica_info.get("path", "")
                )
                replica_data.append(replica)
            
            # 创建StatInfo对象
            stat_info = StatInfo(
                path=data.get("path", ""),
                size=data.get("size", 0),
                mtime=data.get("mtime", 0),
                file_type=FileType.get(data.get("type", 0)),
                replica_data=replica_data
            )
            
            logger.debug(f"Got file stats for {path}: {stat_info}")
            return stat_info
            
        except Exception as e:
            logger.error(f"Failed to get file stats for {path}: {e}")
            return None
    
    def list_file_stats(self, path: str) -> List[StatInfo]:
        """
        列出目录下的文件状态信息
        
        Args:
            path: 目录路径
            
        Returns:
            文件状态信息列表
        """
        try:
            meta_server = self.zk_util.get_master_meta_server()
            if not meta_server:
                raise RuntimeError("No available MetaServer")
            
            url = f"{meta_server.get_url()}/directory/list"
            params = {
                "path": path,
                "fileSystemName": self.default_file_system_name
            }
            
            response = self._call_remote("GET", url, params=params)
            if response.status_code != 200:
                return []
            
            data = response.json()
            stat_list = []
            
            for item in data.get("items", []):
                # 解析副本数据
                replica_data = []
                for replica_info in item.get("replicaData", []):
                    replica = ReplicaData(
                        replica_id=replica_info.get("id", ""),
                        ds_node=replica_info.get("dsNode", ""),
                        path=replica_info.get("path", "")
                    )
                    replica_data.append(replica)
                
                # 创建StatInfo对象
                stat_info = StatInfo(
                    path=item.get("path", ""),
                    size=item.get("size", 0),
                    mtime=item.get("mtime", 0),
                    file_type=FileType.get(item.get("type", 0)),
                    replica_data=replica_data
                )
                stat_list.append(stat_info)
            
            logger.debug(f"Listed {len(stat_list)} items in {path}")
            return stat_list
            
        except Exception as e:
            logger.error(f"Failed to list directory {path}: {e}")
            return []
    
    def get_cluster_info(self) -> ClusterInfo:
        """
        获取集群信息
        
        Returns:
            集群信息
        """
        try:
            # 从ZooKeeper获取集群信息
            master_meta_server = self.zk_util.get_master_meta_server()
            slave_meta_server = self.zk_util.get_slave_meta_server()
            data_servers = self.zk_util.get_data_servers()
            
            cluster_info = ClusterInfo(
                master_meta_server=master_meta_server,
                slave_meta_server=slave_meta_server,
                data_servers=data_servers
            )
            
            logger.info(f"Got cluster info: {len(data_servers)} data servers")
            return cluster_info
            
        except Exception as e:
            logger.error(f"Failed to get cluster info: {e}")
            return ClusterInfo()
    
    def exists(self, path: str) -> bool:
        """
        检查文件或目录是否存在
        
        Args:
            path: 路径
            
        Returns:
            是否存在
        """
        try:
            stat_info = self.get_file_stats(path)
            return stat_info is not None
        except Exception as e:
            logger.error(f"Failed to check existence of {path}: {e}")
            return False
    
    def is_file(self, path: str) -> bool:
        """
        检查是否为文件
        
        Args:
            path: 路径
            
        Returns:
            是否为文件
        """
        try:
            stat_info = self.get_file_stats(path)
            return stat_info is not None and stat_info.is_file()
        except Exception as e:
            logger.error(f"Failed to check if {path} is file: {e}")
            return False
    
    def is_directory(self, path: str) -> bool:
        """
        检查是否为目录
        
        Args:
            path: 路径
            
        Returns:
            是否为目录
        """
        try:
            stat_info = self.get_file_stats(path)
            return stat_info is not None and stat_info.is_directory()
        except Exception as e:
            logger.error(f"Failed to check if {path} is directory: {e}")
            return False
    
    def close(self):
        """关闭文件系统客户端"""
        try:
            self.zk_util.disconnect()
            logger.info("Closed EFileSystem")
        except Exception as e:
            logger.error(f"Failed to close EFileSystem: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
