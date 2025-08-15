"""
文件系统抽象基类
定义了通用的文件系统方法和变量
整体的文件组织结构为以下形式
{namespace}/{dir}
                 /{subdir}
                 /{subdir}/file
                 /file
"""

from typing import Optional
from util.http_client_util import HttpClientUtil


class FileSystem:
    """
    文件系统抽象基类
    定义了通用的文件系统方法和变量
    """
    
    def __init__(self):
        # 文件系统名称，可理解成命名空间，可以存在多个命名空间，多个命名空间下的文件目录结构是独立的
        self.default_file_system_name: str = "default"
        self.http_client: Optional[HttpClientUtil] = None
    
    def _call_remote(self, method: str, url: str, **kwargs):
        """
        远程调用方法
        封装HTTP请求调用MetaServer和DataServer
        """
        if not self.http_client:
            self.http_client = HttpClientUtil()
        
        if method.upper() == 'GET':
            return self.http_client.get(url, **kwargs)
        elif method.upper() == 'POST':
            return self.http_client.post(url, **kwargs)
        elif method.upper() == 'PUT':
            return self.http_client.put(url, **kwargs)
        elif method.upper() == 'DELETE':
            return self.http_client.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
    
    def _get_meta_server_url(self) -> str:
        """
        获取MetaServer的URL
        这里应该通过ZooKeeper获取MetaServer信息
        """
        # TODO: 从ZooKeeper获取MetaServer地址
        return "http://localhost:8001"  # 默认地址
    
    def _get_data_server_url(self, host: str, port: int) -> str:
        """
        获取DataServer的URL
        """
        return f"http://{host}:{port}"
