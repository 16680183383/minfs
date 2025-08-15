"""
ZooKeeper工具类
"""

import logging
from typing import List, Optional, Callable
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, ConnectionLossException
from domain.meta_server_msg import MetaServerMsg
from domain.data_server_msg import DataServerMsg

logger = logging.getLogger(__name__)


class ZkUtil:
    """ZooKeeper工具类"""
    
    def __init__(self, hosts: str = "localhost:2181", timeout: int = 30):
        """
        初始化ZooKeeper工具
        
        Args:
            hosts: ZooKeeper服务器地址
            timeout: 连接超时时间（秒）
        """
        self.hosts = hosts
        self.timeout = timeout
        self.zk: Optional[KazooClient] = None
        self.meta_servers: List[MetaServerMsg] = []
        self.data_servers: List[DataServerMsg] = []
        self.cluster_change_callbacks: List[Callable] = []
    
    def connect(self):
        """连接ZooKeeper"""
        try:
            self.zk = KazooClient(hosts=self.hosts, timeout=self.timeout)
            self.zk.start()
            logger.info(f"Connected to ZooKeeper: {self.hosts}")
            
            # 设置监听器
            self._setup_watchers()
            
        except Exception as e:
            logger.error(f"Failed to connect to ZooKeeper: {e}")
            raise
    
    def disconnect(self):
        """断开ZooKeeper连接"""
        if self.zk:
            self.zk.stop()
            self.zk.close()
            logger.info("Disconnected from ZooKeeper")
    
    def _setup_watchers(self):
        """设置监听器"""
        if not self.zk:
            return
        
        # 监听MetaServer节点
        meta_server_path = "/minfs/metaservers"
        try:
            self.zk.ensure_path(meta_server_path)
            self.zk.ChildrenWatch(meta_server_path, self._on_meta_servers_changed)
        except Exception as e:
            logger.warning(f"Failed to setup MetaServer watcher: {e}")
        
        # 监听DataServer节点
        data_server_path = "/minfs/dataservers"
        try:
            self.zk.ensure_path(data_server_path)
            self.zk.ChildrenWatch(data_server_path, self._on_data_servers_changed)
        except Exception as e:
            logger.warning(f"Failed to setup DataServer watcher: {e}")
    
    def _on_meta_servers_changed(self, children):
        """MetaServer节点变化回调"""
        logger.info(f"MetaServer nodes changed: {children}")
        self._update_meta_servers()
        self._notify_cluster_changed()
    
    def _on_data_servers_changed(self, children):
        """DataServer节点变化回调"""
        logger.info(f"DataServer nodes changed: {children}")
        self._update_data_servers()
        self._notify_cluster_changed()
    
    def _update_meta_servers(self):
        """更新MetaServer列表"""
        if not self.zk:
            return
        
        try:
            meta_server_path = "/minfs/metaservers"
            children = self.zk.get_children(meta_server_path)
            
            self.meta_servers.clear()
            for child in children:
                try:
                    data, _ = self.zk.get(f"{meta_server_path}/{child}")
                    if data:
                        # 解析MetaServer信息
                        # 假设数据格式为: "host:port"
                        host_port = data.decode('utf-8')
                        if ':' in host_port:
                            host, port_str = host_port.split(':', 1)
                            port = int(port_str)
                            meta_server = MetaServerMsg(host=host, port=port)
                            self.meta_servers.append(meta_server)
                except Exception as e:
                    logger.warning(f"Failed to parse MetaServer data for {child}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to update MetaServer list: {e}")
    
    def _update_data_servers(self):
        """更新DataServer列表"""
        if not self.zk:
            return
        
        try:
            data_server_path = "/minfs/dataservers"
            children = self.zk.get_children(data_server_path)
            
            self.data_servers.clear()
            for child in children:
                try:
                    data, _ = self.zk.get(f"{data_server_path}/{child}")
                    if data:
                        # 解析DataServer信息
                        # 假设数据格式为JSON: {"host": "localhost", "port": 8002, "capacity": 1000, ...}
                        import json
                        server_info = json.loads(data.decode('utf-8'))
                        data_server = DataServerMsg(
                            host=server_info.get('host', ''),
                            port=server_info.get('port', 0),
                            file_total=server_info.get('fileTotal', 0),
                            capacity=server_info.get('capacity', 0),
                            use_capacity=server_info.get('useCapacity', 0)
                        )
                        self.data_servers.append(data_server)
                except Exception as e:
                    logger.warning(f"Failed to parse DataServer data for {child}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to update DataServer list: {e}")
    
    def get_meta_servers(self) -> List[MetaServerMsg]:
        """获取MetaServer列表"""
        if not self.meta_servers:
            self._update_meta_servers()
        return self.meta_servers.copy()
    
    def get_data_servers(self) -> List[DataServerMsg]:
        """获取DataServer列表"""
        if not self.data_servers:
            self._update_data_servers()
        return self.data_servers.copy()
    
    def get_master_meta_server(self) -> Optional[MetaServerMsg]:
        """获取主MetaServer"""
        meta_servers = self.get_meta_servers()
        if meta_servers:
            # 简单策略：选择第一个作为主服务器
            # 实际应该根据ZooKeeper中的选举信息
            return meta_servers[0]
        return None
    
    def get_slave_meta_server(self) -> Optional[MetaServerMsg]:
        """获取从MetaServer"""
        meta_servers = self.get_meta_servers()
        if len(meta_servers) > 1:
            # 简单策略：选择第二个作为从服务器
            return meta_servers[1]
        return None
    
    def register_cluster_change_callback(self, callback: Callable):
        """注册集群变化回调"""
        self.cluster_change_callbacks.append(callback)
    
    def _notify_cluster_changed(self):
        """通知集群变化"""
        for callback in self.cluster_change_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Cluster change callback failed: {e}")
    
    def create_meta_server_node(self, host: str, port: int):
        """创建MetaServer节点"""
        if not self.zk:
            return
        
        try:
            meta_server_path = "/minfs/metaservers"
            self.zk.ensure_path(meta_server_path)
            
            # 创建临时节点
            node_data = f"{host}:{port}".encode('utf-8')
            self.zk.create(f"{meta_server_path}/meta-", node_data, ephemeral=True, makepath=True)
            logger.info(f"Created MetaServer node: {host}:{port}")
            
        except Exception as e:
            logger.error(f"Failed to create MetaServer node: {e}")
    
    def create_data_server_node(self, host: str, port: int, capacity: int = 1000):
        """创建DataServer节点"""
        if not self.zk:
            return
        
        try:
            data_server_path = "/minfs/dataservers"
            self.zk.ensure_path(data_server_path)
            
            # 创建临时节点
            node_data = {
                "host": host,
                "port": port,
                "fileTotal": 0,
                "capacity": capacity,
                "useCapacity": 0
            }
            import json
            self.zk.create(f"{data_server_path}/data-", json.dumps(node_data).encode('utf-8'), 
                          ephemeral=True, makepath=True)
            logger.info(f"Created DataServer node: {host}:{port}")
            
        except Exception as e:
            logger.error(f"Failed to create DataServer node: {e}")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
